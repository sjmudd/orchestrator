/*
   Copyright 2014 Outbrain Inc.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package caretaker

import (
	"time"

	"github.com/github/orchestrator/go/inst"
	"github.com/github/orchestrator/go/process"
	"github.com/openark/golib/log"
)

const caretakerTickPeriod = time.Minute

// Caretaker is an object which runs periodic maintenance tasks within orchestrator.
type Caretaker struct {
	c chan struct{}
}

// NewCaretaker returns a pointer to a new Caretaker
func NewCaretaker() *Caretaker {
	return &Caretaker{
		c: make(chan struct{}, 0),
	}
}

// Stop signals to the caretaker it should finish
func (caretaker *Caretaker) Stop() {
	log.Infof("Stopping caretaker routine")
	caretaker.c <- struct{}{}
}

// Run executes the caretaker process continuously
func (caretaker *Caretaker) Run() {
	log.Infof("Starting caretaker routine")

	// FIXME: The caretaker tick period is fixed and should
	// depend on reading from the config. Usually not a big deal.
	caretakingTick := time.Tick(caretakerTickPeriod)
ForLoop:
	for {
		select {
		case <-caretaker.c:
			// can't stop or free caretakingTick (as per docs)
			break ForLoop
		case <-caretakingTick:
			// Various periodic internal maintenance tasks

			_, isElected, err := process.ElectedNode()
			if err != nil {
				// report the error and skip this iteration
				log.Warningf("Caretaker unable to determine if elected node: %v", err)
				continue
			}

			if isElected {
				go inst.RecordInstanceBinlogFileHistory()
				go inst.ForgetLongUnseenInstances()
				go inst.ForgetUnseenInstancesDifferentlyResolved()
				go inst.ForgetExpiredHostnameResolves()
				go inst.DeleteInvalidHostnameResolves()
				go inst.ReviewUnseenInstances()
				go inst.InjectUnseenMasters()
				go inst.ResolveUnknownMasterHostnameResolves()
				go inst.ExpireMaintenance()
				go inst.ExpireCandidateInstances()
				go inst.ExpireHostnameUnresolve()
				go inst.ExpireClusterDomainName()
				go inst.ExpireAudit()
				go inst.ExpireMasterPositionEquivalence()
				go inst.ExpirePoolInstances()
				go inst.FlushNontrivialResolveCacheToDatabase()
				go process.ExpireNodesHistory()
				go process.ExpireAccessTokens()
				go process.ExpireAvailableNodes()
			}
		}
	}
	log.Infof("Caretaker routine stopped")
}
