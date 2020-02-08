package etcd_wrapper

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	v3 "github.com/coreos/etcd/clientv3"
	"github.com/sirupsen/logrus"
	cc "go.etcd.io/etcd/clientv3/concurrency"
)

type ElectionWrapper struct {
	// Etcd addr
	EtcdAddr string
	// split with ','
	etcdAddrSlice []string
	// default 5s
	Timeout int64
	// ElectionID, for distinguish different election
	ElectionID string
	// CampaignName, identified the campaign value
	CampaignName string
	// default 10s
	LeaseTTL int64
	// leader value
	leaderName string
	// last data time
	lastDataTime int64
	// lock
	lock sync.RWMutex
	//
	RefreshLeaderInfoTime int64
}

func (e *ElectionWrapper) Init() (err error) {
	// check if the paras is valid
	if len(e.EtcdAddr) != 0 && (len(e.ElectionID) == 0 || len(e.CampaignName) == 0) {
		err = fmt.Errorf("ElectionWrapper:%+v,has invalid item", *e)
		logrus.Errorf("Init,error:%s", err.Error())
		return
	}
	if e.Timeout < 0 || e.LeaseTTL < 0 {
		err = fmt.Errorf("ElectionWrapper:%+v,has invalid item", *e)
		logrus.Errorf("Init,error:%s", err.Error())
		return
	}
	if e.Timeout == 0 {
		e.Timeout = DefaultTimeout
	}
	if e.LeaseTTL == 0 {
		e.LeaseTTL = DefaultLeaseTTL
	}
	if e.RefreshLeaderInfoTime == 0 {
		e.RefreshLeaderInfoTime = DefaultRefreshLeaderInfoTime
	}
	e.etcdAddrSlice = strings.Split(e.EtcdAddr, ",")
	go e.watchLeader()
	go e.runElection()
	return
}

func (e *ElectionWrapper) refreshData(name string) {
	e.lock.Lock()
	e.leaderName = name
	e.lastDataTime = time.Now().Unix()
	e.lock.Unlock()
}

// watchLeader, go routine function
func (e *ElectionWrapper) watchLeader() {
	logger := logrus.WithFields(logrus.Fields{
		"watchLeader": fmt.Sprintf("%+v", *e),
	})
	cli, err := v3.New(v3.Config{
		DialTimeout: time.Duration(e.Timeout) * time.Second,
		Endpoints:   e.etcdAddrSlice,
	})
	if err != nil {
		logger.Errorf("watchLeader,error:%s", err.Error())
		panic(fmt.Sprintf("watchLeader,error:%s", err.Error()))
	}
	ss, err := cc.NewSession(cli)
	if err != nil {
		logger.Errorf("watchLeader,new session errror:%s", err.Error())
		panic(fmt.Sprintf("watchLeader,new session error:%s", err.Error()))
	}
	defer ss.Close()
	for {
		time.Sleep(time.Duration(e.RefreshLeaderInfoTime) * time.Second)
		elec := cc.NewElection(ss, e.ElectionID)
		ctx, _ := context.WithTimeout(context.TODO(), time.Duration(e.Timeout)*time.Second)
		lead, err := elec.Leader(ctx)
		if err != nil {
			logger.Errorf("watchLeader,get leader,error:%s", err.Error())
		} else {
			kv := lead.Kvs[0]
			logger.Debugf("watchLeader,leader info:%+v", kv)
			e.refreshData(string(kv.Value))
		}
	}
}

func (e ElectionWrapper) runElection() {
	logger := logrus.WithFields(logrus.Fields{
		"raw": fmt.Sprintf("%+v", e),
	})
	//logger.Debug()
	cli, err := v3.New(v3.Config{
		DialTimeout: time.Duration(e.Timeout) * time.Second,
		Endpoints:   e.etcdAddrSlice,
	})
	if err != nil {
		logger.Errorf("watchLeader,new error:%s", err.Error())
		panic(fmt.Sprintf("watchLeader,new error:%s", err.Error()))
	}
	ss, err := cc.NewSession(cli)
	if err != nil {
		logger.Errorf("watchLeader,new session errror:%s", err.Error())
		panic(fmt.Sprintf("watchLeader,new session error:%s", err.Error()))
	}
	defer ss.Close()

	//logger.Debug()
	for {
		election := cc.NewElection(ss, e.ElectionID)
		ctx, _ := context.WithTimeout(context.TODO(), time.Duration(e.Timeout)*time.Second)
		_, err = election.Leader(ctx)
		if err != nil {
			// 租约授权
			lease := v3.NewLease(cli)
			leaseResp, err := lease.Grant(ctx, e.LeaseTTL)
			if err != nil {
				logger.Errorf("runElection,set lease timeout error:%s", err.Error())
				continue
			}
			leaseID := leaseResp.ID
			ssNew, err := cc.NewSession(cli, cc.WithLease(leaseID))
			if err != nil {
				logger.Errorf("runElection,NewSession error:%s", err.Error())
				err = ssNew.Close()
				if err != nil {
					logger.Errorf("runElection,NewSession close error:%s", err.Error())
				}
				continue
			}
			// 竞选
			newElection := cc.NewElection(ssNew, e.ElectionID)
			err = newElection.Campaign(ctx, e.CampaignName)
			if err != nil {
				logger.Errorf("runElection,Campaign error:%s", err.Error())
			} else {
				leaseRespChan, err := lease.KeepAlive(ctx, leaseID)
				if err != nil {
					logger.Errorf("runElection,refresh lease error:%s", err.Error())
				}
				go func() {
					for {
						select {
						case leaseKeepResp := <-leaseRespChan:
							if leaseKeepResp == nil {
								logger.Debugf("refresh leasing closed ...")
								return
							} else {
								logger.Debugf("refresh leasing success ...")
								goto END
							}
						}
					END:
						time.Sleep(500 * time.Millisecond)
					}
				}()
			}
			err = ssNew.Close()
			if err != nil {
				logger.Errorf("runElection,NewSession close error:%s", err.Error())
			}
		}
		time.Sleep(time.Second)
	}
}

func (e ElectionWrapper) IsLeader() bool {
	return e.CampaignName == e.leaderName
}
