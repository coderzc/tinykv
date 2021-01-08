package standalone_storage

import (
	"fmt"
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/storage/raft_storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"log"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	badger.Options
	*badger.DB
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	opts := badger.DefaultOptions
	opts.Dir = conf.DBPath
	opts.ValueDir = conf.DBPath
	standAloneStorage := &StandAloneStorage{Options: opts}
	return standAloneStorage
}

func (s *StandAloneStorage) Start() error {
	if db, err := badger.Open(s.Options); err != nil {
		log.Fatal(err)
		return err
	} else {
		log.Println("db open success!")
		s.DB = db
	}
	return nil
}

func (s *StandAloneStorage) Stop() error {
	err := s.DB.Close()
	if err != nil {
		log.Fatal(err)
	}
	return err
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	region := metapb.Region{
		Id:          ctx.RegionId,
		RegionEpoch: ctx.RegionEpoch,
		Peers:       []*metapb.Peer{ctx.Peer},
	}
	txn := s.NewTransaction(false)
	regionReader := raft_storage.NewRegionReader(txn, region)
	return regionReader, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	txn := s.NewTransaction(true)
	defer txn.Commit()
	for _, data := range batch {
		switch data.Data.(type) {
		case storage.Put:
			if err := txn.Set(data.Key(), data.Value()); err != nil {
				return err
			}
		case storage.Delete:
			if err := txn.Delete(data.Key()); err != nil {
				return err
			}
		default:
			return fmt.Errorf("data.Data type not support")
		}
	}
	return nil
}
