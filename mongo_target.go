package main

import (
	"fmt"
	"labix.org/v2/mgo"
	"labix.org/v2/mgo/bson"
	"net/url"
	"reflect"
	"regexp"
	"strings"
	"sync"
	"time"
)

type MongoTarget struct {
	dst    *mgo.Session
	dstURI *url.URL
	dstDB  string

	src    *mgo.Session
	srcURI *url.URL
	srcDB  string

	result ApplyOpResult
}

// NewMongoTarget creates a new Mongodb Target
func NewMongoTarget(uri *url.URL, dstDB string) *MongoTarget {
	return &MongoTarget{dstURI: uri, dstDB: dstDB}
}

// Dial connect to mongo, and return an error if there's a problem
func (t *MongoTarget) Dial() error {
	dst, err := mgo.Dial(t.dstURI.String())
	if err != nil {
		return fmt.Errorf("Cannot dial %s\n, %v", t.dstURI, err)
	}
	t.dst = dst
	return nil
}

func (t *MongoTarget) DB(db string) {
	t.dstDB = db
}

func (t *MongoTarget) Close() {
	t.dst.Close()
}

// KeepAlive sends a ping to mongo, and reconnects if there's a failure
func (t *MongoTarget) KeepAlive() error {
	if err := t.dst.Ping(); err != nil {
		t.dst, err = mgo.Dial(t.dstURI.String())
		if err != nil {
			return err
		}
	}
	return nil
}

// Apply an oplog document to the destination database
func (m *MongoTarget) Apply(ops []OplogDoc) (ApplyOpResult, error) {
	applied := 0
	for _, op := range ops {
		err := m.ApplyOne(op)
		if err == nil {
			applied++
		}
	}
	return ApplyOpResult{Ok: 1, Applied: applied}, nil
}


// Apply one operation by breaking it open, constructing the proper operation and running
// it manually
func (m MongoTarget) ApplyOne(op OplogDoc) (err error) {
	logger.Finest("%s Op: %+v", op.String(), op)
	switch op.Kind() {
	case INSERT:
		if op.Collection() == "system.indexes" || op.Collection() == "system.users" {
			logger.Debug("Adding Index Op")
			ns, exists := op.O["ns"]
			if exists {
				op.O["ns"] = strings.Replace(ns.(string), m.srcDB, m.dstDB, 1)
			}
		}
		return m.dst.DB(op.Database()).C(op.Collection()).Insert(op.O)
	case UPDATE:
		return m.dst.DB(op.Database()).C(op.Collection()).Update(op.O2, op.O);
	case DELETE:
		return m.dst.DB(op.Database()).C(op.Collection()).Remove(op.O)
	case COMMAND:
		ns, exists := op.O["ns"]
		if exists {
			op.O["ns"] = strings.Replace(ns.(string), m.srcDB, m.dstDB, 1)
		}
		result := bson.M{}
		err = m.dst.DB(op.Database()).Run(op.O, &result)
		ok, exists := result["ok"]
		if (err == nil) && (exists && ok == 1) {
			return nil
		} 
		return fmt.Errorf("Err: %v, ok: %d", err, ok)
	}
	return fmt.Errorf("unrecognized command %v", op.Op)
}

// Sync
// so the basic plan of attack is as follows
// get a list of collections, and for each collection do:
// - nuke the collection on the destination
// - create the collection on the dest, with the appropraite options,
// - i.e. capped collections, etc
// - iterate over the data on the src, and insert in bulk into dest
// - keep track of checkpoints, in case the range over the source data fails

type CollectionSyncTracker struct {
	Name string
	Err  error
}

func (t *MongoTarget) Sync(src *mgo.Session, srcURI *url.URL, srcDB string) (err error) {
	t.src = src
	t.srcURI = srcURI
	t.srcDB = srcDB

	t.dst.EnsureSafe(&mgo.Safe{})
	t.dst.SetBatch(1000)
	t.dst.SetPrefetch(0.5)

	names, err := src.DB(t.srcDB).CollectionNames()
	if err != nil {
		return err
	}

	// delete the destination collections
	for _, v := range names {
		if strings.HasPrefix(v, "system.") {
			continue
		}
		// drop collection
		t.dst.DB(t.dstDB).C(v).DropCollection()
	}

	// sync the indexes with unique indexes
	err = t.SyncIndexes(true)
	if err != nil {
		return err
	}
	if *forceIndexBuild == "im" {
		err = t.SyncIndexes(false)
		if err != nil {
			return err
		}
	}

	t.src.Refresh()
	err = t.SyncUsers()
	if err != nil {
		return err
	}

	chcol := make(chan CollectionSyncTracker)
	expected := 0
	for _, v := range names {
		if strings.HasPrefix(v, "system.") {
			continue
		}
		expected++
		logger.Finest("Launching goroutine to copy collection %s", v)
		go func(c string) {
			err := t.SyncCollection(c)
			if err != nil {
				chcol <- CollectionSyncTracker{c, err}
			} else {
				chcol <- CollectionSyncTracker{c, nil}
			}
		}(v)
		time.Sleep(400 * time.Millisecond) // pause a little bit, just to be polite
	}

	for i := 0; i < expected; i++ {
		ret := <-chcol
		if ret.Err != nil {
			return ret.Err
		} else {
			logger.Info("Finished collection %s, %d collections remaining", ret.Name, expected-i-1)
		}
	}

	if *forceIndexBuild != "im" {
		err = t.SyncIndexes(false)
		if err != nil {
			return err
		}
	}

	if !*ignore_errors {
		t.src.Refresh()
		if err = t.Validate(); err != nil {
			return fmt.Errorf("CopyDatabase Failed %s", err.Error())
		}
	}

	return nil
}

// bundle up the op and the error we're sending off to our workers
type OpChannels struct {
	chop  chan InsertMessage
	cherr chan error
}

func newOpChannels() *OpChannels {
	return &OpChannels{make(chan InsertMessage), make(chan error)}
}

// syncs a collection, and makes sense of the return values
func (t *MongoTarget) SyncCollection(collection string) (err error) {

	// copy collection information
	collInfo, err := CollectionInfo(t.src.DB(t.srcDB).C(collection))
	if err != nil {
		return fmt.Errorf("Can't get collection info: %s", err.Error())
	}

	if collInfo.Capped {
		t.dst.DB(t.dstDB).C(collection).Create(collInfo)
	}

	count, err := t.src.DB(t.srcDB).C(collection).Count()
	if err != nil {
		return err
	}

	// set up goroutines and channels to handle the inserts
	var (
		quit       = make(chan bool)
		chdone     = make(chan int, 1)
		cierr      = make(chan error)
		wg         = new(sync.WaitGroup)
		opChannels = newOpChannels()
	)

	for i := 0; i < GOPOOL_SIZE; i++ {
		go writeBuffer(t.dst.New().DB(t.dstDB), opChannels)
	}

	wg.Add(1)
	go t.collectionIterator(t.src.Clone(), collection, count, opChannels, chdone, quit, cierr, wg)

	var (
		got      = 0
		total    = 0
		finished = false
	)

A:
	for {
		select {
		case err = <-cierr:
			wg.Wait()
			err = fmt.Errorf("copy %s failed: %v", collection, err)
			break A
		case err = <-opChannels.cherr:
			if err != nil {
				quit <- true
				wg.Wait()
				err = fmt.Errorf("failed insert on collection %s: %s", collection, err)
				break A
			}
			got++
			if finished && got >= total {
				logger.Debug("%s complete, %d buffers written", collection, got)
				break A
			}
		case total = <-chdone:
			logger.Finest("Queued %d writes on %s.  Waiting for %d stragglers", total, collection, total-got)
			finished = true
			if got >= total {
				logger.Debug("%s complete, %d buffers written", collection, got)
				break A
			}
		}
	}
	close(opChannels.chop)
	return err
}

func (t *MongoTarget) collectionIterator(sess *mgo.Session, collection string, count int, opChannels *OpChannels, chdone chan int, quit chan bool, cierr chan error, wg *sync.WaitGroup) {
	logger.Info("Syncing %d documents from %s", count, collection)
	defer wg.Done()

	var (
		buffer     = make([]interface{}, 0, BUFFER_SIZE)
		doc        = bson.M{}
		idx        = 0
		sent       = 0
		currentId  interface{} //bson.ObjectId
		bufferSize = 0
	)

	// we use different iterators depending on whether or not we have a non ObjectID _id index
	var iter *mgo.Iter
	if *forceTableScan {
		iter = sess.DB(t.srcDB).C(collection).Find(bson.M{}).Sort("$natural").Iter()
	} else {
		iter = sess.DB(t.srcDB).C(collection).Find(bson.M{}).Sort("_id").Iter()
	}

A:
	for iter.Next(&doc) {
		// we make some assumptions here
		// we are assuming that there is a field '_id',
		// and we are assuming that all documents with id are indexed
		// - this can be a problem, compare db.collection.stat() with db.collection.count()

		if err := iter.Err(); err != nil {
			if *forceTableScan {
				logger.Error("got an error reading from source, unable to refresh.  %v", err)
				cierr <- err
				break
			}

			logger.Error("got error (%v) reseting connection, sleeping for 60 seconds, please stand by...", err)
			time.Sleep(60 * time.Second)
			sess.Refresh()
			logger.Info("connection 'refreshed', issuing query")
			iter = t.src.New().DB(t.srcDB).C(collection).Find(bson.M{"_id": bson.M{"$gt": currentId}}).Sort("_id").Iter()
			continue
		}

		select {
		case <-quit:
			break A
		default:
		}

		if !*forceTableScan {
			id, ok := doc["_id"]
			if !ok {
				logger.Error("got an error reading _id field from the %s collection.  retry seed with -forceTableScan", collection)
				cierr <- fmt.Errorf("document has no _id index")
				break A
			} else {
				currentId = id
				switch t := id.(type) {
				case bson.ObjectId, int64, float64:
					// ok
				case string:
					if len(t) >= 1024 {
						logger.Debug("_id: \"%s\" too large to index", id)
						logger.Error("collection %s has a _id type of string which is over 1024 bytes, retry seed with -forceTableScan", collection)
						cierr <- fmt.Errorf("non ObjectID _id field")
						break A
					}
				default:
					logger.Error("collection %s has a non ObjectID _id index, retry seed with -forceTableScan", collection)
					cierr <- fmt.Errorf("non ObjectID _id field")
					break A
				}
			}
		}

		// crucial
		// if we have room in the buffer, then add the doc
		// otherwise, send it off to be written
		sz, er := docSize(doc)
		if er != nil {
			logger.Error("can't find doc size, %v", er)
			return
		}
		if ((sz + bufferSize) > MAX_BUFFER_SIZE) || (len(buffer) > BUFFER_SIZE) {
			// send it off to be inserted
			logger.Finest("pushing writes: bufferSize: %d, count: %d", bufferSize+sz, len(buffer))
			opChannels.chop <- InsertMessage{ops: buffer, coll: collection}
			sent++
			buffer = make([]interface{}, 0, BUFFER_SIZE)
			bufferSize = 0
		}

		buffer = append(buffer, doc)
		bufferSize += sz

		// dump some stats periodically
		idx++
		if idx%1000 == 0 {
			logger.Info("- %s (%d/%d) %.2f%% complete", collection, idx, count, float64(idx)/float64(count)*100)
		}

		doc = make(bson.M)
	}
	if len(buffer) > 0 {
		opChannels.chop <- InsertMessage{ops: buffer, coll: collection}
		sent++
	}
	chdone <- sent
}

//InsertMessage is passed along a channel to a goroutine, and contains the buffer to write to the target
type InsertMessage struct {
	ops  []interface{}
	doc  bson.M
	coll string
}

var isIdIndexRegex = regexp.MustCompile(`\$_id_ `)

func isIdIndex(s string) bool {
	return isIdIndexRegex.MatchString(s)
}

func writeBuffer(db *mgo.Database, opChannels *OpChannels) {
	var err error
	for msg := range opChannels.chop {
		logger.Finest("Inserting %d documents", len(msg.ops))
		err = db.C(msg.coll).Insert(msg.ops...)
		if err != nil {
			logger.Error("insert error (%v)", err)
			for _, o := range msg.ops {
				logger.Debug("%v", o)
			}
		}
		if err != nil {
			if mgo.IsDup(err) {
				logger.Debug("Non-fatal Insert error %v, continuing", err)
				err = nil
				for _, m := range msg.ops {
					e := db.C(msg.coll).Insert(m)
					if mgo.IsDup(e) {
						continue
					}
					err = e
					break
				}
			}
		}
		opChannels.cherr <- err
	}
}

// SyncIndexes copies the indexes from the source to the destination
func (t *MongoTarget) SyncIndexes(unique bool) error {
	if unique {
		logger.Info("Adding unique indexes")
	} else {
		logger.Info("Adding non-unique indexes")
	}
	var query bson.M
	if unique {
		query = bson.M{"unique": true}
	} else {
		query = bson.M{"unique": bson.M{"$ne": true}}
	}

	doc := bson.M{}
	iter := t.src.DB(t.srcDB).C("system.indexes").Find(query).Iter()
	for iter.Next(&doc) {
		ns, ok := doc["ns"].(string)
		if !ok {
			continue
		}

		if strings.HasPrefix(ns, t.srcDB+".system.") {
			continue
		}

		if *forceIndexBuild == "fg" || *forceIndexBuild == "bg" {
			_, ok := doc["background"]
			if ok {
				if *forceIndexBuild == "fg" {
					doc["background"] = false
				} else {
					doc["background"] = true
				}
			}
		}

		logger.Debug("Adding index %+v", doc)
		doc["ns"] = strings.Replace(ns, t.srcDB, t.dstDB, 1)
		err := t.dst.DB(t.dstDB).C("system.indexes").Insert(doc)
		if err != nil {
			if err.Error() == "index with name _id_ already exists" {
				continue
			}
			return fmt.Errorf("Could not add index: %s", err.Error())
		}
	}
	return nil
}

// SyncUsers copies the indexes from the source to the destination
func (t *MongoTarget) SyncUsers() error {
	logger.Info("Adding users")
	count := 0
	doc := bson.M{}
	iter := t.src.DB(t.srcDB).C("system.users").Find(bson.M{}).Iter()
	for iter.Next(&doc) {
		logger.Finest("adding user: %+v", doc)
		c, _ := t.dst.DB(t.dstDB).C("system.users").Find(doc).Count()
		if c == 0 {
			t.dst.DB(t.dstDB).C("system.users").Insert(doc)
			count++
		}
	}
	logger.Debug("%d Users added", count)
	return nil
}

func (t *MongoTarget) Validate() error {
	src := t.src.DB(t.srcDB)
	dst := t.dst.DB(t.dstDB)

	// check indexes
	doc := bson.M{}
	iter := src.C("system.indexes").Find(bson.M{}).Iter()
	for iter.Next(&doc) {
		ns, ok := doc["ns"].(string)
		if !ok {
			return fmt.Errorf("failed to copy indexes, namespace not found")
		}

		if strings.HasPrefix(ns, t.dstDB+".system.") {
			logger.Debug("Ignoring index on system collection %s", ns)
			continue
		}

		doc["ns"] = strings.Replace(ns, t.srcDB, t.dstDB, 1)
		c, err := dst.C("system.indexes").Find(doc).Count()
		if err != nil {
			return fmt.Errorf("failed to copy indexes, got %s", err.Error())
		}
		if c != 1 {
			return fmt.Errorf("failed to copy indexes, %d - %v", c, doc)
		}
	}

	// check collections
	names, err := src.CollectionNames()
	if err != nil {
		return fmt.Errorf("Couldn't get collection names %s", err.Error())
	}
	for _, collection := range names {
		if strings.HasPrefix(collection, "system.") {
			continue
		}

		c1, err := src.C(collection).Count()
		if err != nil {
			return err
		}

		c2, err := dst.C(collection).Count()
		if err != nil {
			return err
		}

		if c1 != c2 {
			return fmt.Errorf("collection %s has a different count, got: %d, expected %d", collection, c2, c1)
		}

		// check collectionInfo e.g. capped collection, etc
		details1, err := CollectionInfo(src.C(collection))
		if err != nil {
			return fmt.Errorf("Couldn't get collection info from src for %s", collection)
		}

		details2, err := CollectionInfo(dst.C(collection))
		if err != nil {
			return fmt.Errorf("Couldn't get collection info from dst for ", collection)
		}
		if !reflect.DeepEqual(details1, details2) {
			return fmt.Errorf("Collection %s created with different options, failed", collection)
		}
		logger.Debug("%s details ok, Count: %d, Capped: %t, Size: %d, Max: %d ", collection, c1, details1.Capped, details1.MaxBytes, details1.MaxDocs)
	}

	logger.Info("Data copied successfully")
	return nil
}
