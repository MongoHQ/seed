#!/bin/sh

echo "current state"
ls -alh /home/ubuntu
ls -alh /home/ubuntu/bin


if [ ! -d $GOROOT ]; then 
  echo "INSTALLING LATEST GO RELEASE"
  cd $HOME
  mkdir -p $GOROOT
  hg clone -u release https://code.google.com/p/go 
  cd $GOROOT/src
  ./all.bash
  export PATH="$GOROOT/bin:$PATH"
fi

if [ ! -d $HOME/boto ]; then
  echo "INSTALLING BOTO"
  pip install -t $HOME/boto boto
fi

if [ ! -f $HOME/bin/bzr ]; then
  echo "INSTALLING BZR"
  cd $HOME
  wget https://launchpad.net/bzr/2.6/2.6b2/+download/bzr-2.6b2.tar.gz
  tar zxvf bzr-2.6b2.tar.gz
  cd $HOME/bzr-2.6b2; 
  python setup.py install --home $HOME
fi


if [ ! -d $GOPATH ]; then
  echo "setting up gopath"

  #set up GOPATH
  mkdir -p $GOPATH/src
  mkdir -p $GOPATH/pkg
  mkdir -p $GOPATH/bin

  go get -d -v labix.org/v2/mgo
  go get -d -v labix.org/v2/mgo/bson
  go get -d -v code.google.com/p/log4go
fi


