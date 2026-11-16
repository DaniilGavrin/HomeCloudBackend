// Package storageagent owns the active HomeCloud Storage Service sessions and
// request/response correlation. IDs and epochs are non-secret routing labels.
package storageagent

import (
 "context"
 "encoding/json"
 "errors"
 "fmt"
 "sync"
 "time"
)
var ErrNotRegistered=errors.New("STORAGE_AGENT_NOT_REGISTERED")
var ErrAgentStale=errors.New("STORAGE_AGENT_OFFLINE")
var ErrDuplicate=errors.New("DUPLICATE_STORAGE_NODE")
var ErrWrongSession=errors.New("STALE_STORAGE_SESSION")
var ErrNotFound=errors.New("STORAGE_REQUEST_NOT_FOUND")

type Entry struct {
 NodeID string `json:"storage_node_id"`
 BootID string `json:"boot_id"`
 SessionID int64 `json:"-"`
 SessionEpoch uint64 `json:"session_epoch"`
 LastSeen time.Time `json:"last_seen"`
 Online bool `json:"online"`
 Capabilities []string `json:"capabilities"`
 Volumes json.RawMessage `json:"volumes,omitempty"`
 Roots json.RawMessage `json:"roots,omitempty"`
 FileWS json.RawMessage `json:"file_ws,omitempty"`
}
type Result struct { Data json.RawMessage;Err error }
type pending struct{ NodeID string; SessionID int64; Epoch uint64; Answer chan Result }
type Registry struct {
 mu sync.Mutex
 entries map[string]Entry
 pending map[string]pending
 seq uint64
 epoch uint64
}
func New()*Registry{return &Registry{entries:make(map[string]Entry),pending:make(map[string]pending)}}
func(r *Registry)Register(nodeID,bootID string,sessionID int64,capabilities []string)(Entry,error){
 if r==nil||nodeID==""||bootID==""||sessionID==0{return Entry{},errors.New("INVALID_STORAGE_REGISTRATION")}
 r.mu.Lock();defer r.mu.Unlock()
 if e,ok:=r.entries[nodeID];ok&&e.SessionID!=sessionID{return Entry{},ErrDuplicate}
 if e,ok:=r.entries[nodeID];ok&&e.SessionID==sessionID{return e,nil}
 r.epoch++;e:=Entry{NodeID:nodeID,BootID:bootID,SessionID:sessionID,SessionEpoch:r.epoch,LastSeen:time.Now(),Online:true,Capabilities:append([]string(nil),capabilities...)};r.entries[nodeID]=e;return e,nil
}
func(r *Registry)check(nodeID string,sessionID int64,epoch uint64)(Entry,error){e,ok:=r.entries[nodeID];if !ok{return Entry{},ErrNotRegistered};if e.SessionID!=sessionID||e.SessionEpoch!=epoch{return Entry{},ErrWrongSession};return e,nil}
func(r *Registry)Heartbeat(nodeID string,sessionID int64,epoch uint64)error{
 r.mu.Lock();defer r.mu.Unlock();e,err:=r.check(nodeID,sessionID,epoch);if err!=nil{return err};e.LastSeen=time.Now();r.entries[nodeID]=e;return nil
}
func(r *Registry)Inventory(nodeID string,sessionID int64,epoch uint64,volumes,roots json.RawMessage,fileWS ...json.RawMessage)error{
 r.mu.Lock();defer r.mu.Unlock();e,err:=r.check(nodeID,sessionID,epoch);if err!=nil{return err};e.Volumes=append(json.RawMessage(nil),volumes...);e.Roots=append(json.RawMessage(nil),roots...);
 e.FileWS=nil
 if len(fileWS)>0 && len(fileWS[0])>0 && string(fileWS[0])!="null" {e.FileWS=append(json.RawMessage(nil),fileWS[0]...)}
 e.LastSeen=time.Now();r.entries[nodeID]=e;return nil
}
func(r *Registry)Status()[]Entry{
 r.mu.Lock();defer r.mu.Unlock();out:=make([]Entry,0,len(r.entries));for _,e:=range r.entries{e.Online=time.Since(e.LastSeen)<65*time.Second;out=append(out,e)};return out
}
func(r *Registry)NewRequest(nodeID string)(string,uint64,<-chan Result,error){
 r.mu.Lock();defer r.mu.Unlock();e,ok:=r.entries[nodeID];if !ok{return "",0,nil,ErrNotRegistered};if time.Since(e.LastSeen)>=65*time.Second{return "",0,nil,ErrAgentStale}
 r.seq++;id:=fmt.Sprintf("storage-req-%d",r.seq);ch:=make(chan Result,1);r.pending[id]=pending{NodeID:nodeID,SessionID:e.SessionID,Epoch:e.SessionEpoch,Answer:ch};return id,e.SessionEpoch,ch,nil
}
func(r *Registry)Reply(nodeID string,sessionID int64,epoch uint64,requestID string,success bool,data json.RawMessage,errString string)error{
 r.mu.Lock();defer r.mu.Unlock();p,ok:=r.pending[requestID];if !ok{return ErrNotFound};if p.NodeID!=nodeID||p.SessionID!=sessionID||p.Epoch!=epoch{return ErrWrongSession}
 delete(r.pending,requestID);result:=Result{Data:append(json.RawMessage(nil),data...)};if !success{if errString==""{errString="STORAGE_AGENT_OPERATION_FAILED"};result.Err=errors.New(errString)};p.Answer<-result;return nil
}
func(r *Registry)Cancel(requestID string){r.mu.Lock();delete(r.pending,requestID);r.mu.Unlock()}
func(r *Registry)Disconnect(nodeID string,sessionID int64){
 r.mu.Lock();defer r.mu.Unlock();e,ok:=r.entries[nodeID];if !ok||e.SessionID!=sessionID{return};delete(r.entries,nodeID)
 for id,p:=range r.pending{if p.NodeID==nodeID&&p.SessionID==sessionID{delete(r.pending,id);p.Answer<-Result{Err:ErrAgentStale}}}
}
func Wait(ctx context.Context,ch <-chan Result)(json.RawMessage,error){select{case res:=<-ch:return res.Data,res.Err;case<-ctx.Done():return nil,ctx.Err()}}
