package wsserver

import (
 "bytes"
 "context"
 "encoding/json"
 "errors"
 "fmt"
 "io"
 "time"
 "strconv"
 "net/url"
 "net/netip"

 "HomeCloudBackend/internal/commands"
 "HomeCloudBackend/internal/storageagent"
)

func strictStorageAgentData(in json.RawMessage,out any)error {
 d:=json.NewDecoder(bytes.NewReader(in));d.DisallowUnknownFields()
 if err:=d.Decode(out);err!=nil{return fmt.Errorf("INVALID_STORAGE_REQUEST: %w",err)}
 var trailing any;if err:=d.Decode(&trailing);!errors.Is(err,io.EOF){return errors.New("INVALID_STORAGE_REQUEST: trailing JSON")}
 return nil
}

// All agent management stays on Backend's existing control WS (hello/identified).
// storage_node_id is NOT an authentication credential. Published roots are
// restricted by the agent's host-local config, never by remote root creation.
func(s *Server)registerStorageAgentCommands(){
 s.registry.MustRegister("storage.agent.register",s.registerStorageAgent)
 s.registry.MustRegister("storage.agent.heartbeat",s.storageAgentHeartbeat)
 s.registry.MustRegister("storage.agent.inventory",s.storageAgentInventory)
 s.registry.MustRegister("storage.agent.reply",s.storageAgentReply)
 s.registry.MustRegister("storage.agent.status",s.storageAgentStatus)
 for _,name:=range []string{"storage.agent.volumes.list","storage.agent.roots.list","storage.fs.list","storage.fs.stat"}{
  action:=name
  s.registry.MustRegister(action,func(ctx context.Context,m commands.Inbound,caller commands.ClientContext)(any,error){return s.forwardStorageAgent(ctx,m,caller,action)})
 }
}
func(s *Server)registerStorageAgent(_ context.Context,m commands.Inbound,caller commands.ClientContext)(any,error){
 if caller.ClientType!="storage_service"||caller.SessionID==0{return nil,errors.New("STORAGE_AGENT_CLIENT_REQUIRED")}
 var x struct{StorageNodeID string `json:"storage_node_id"`;BootID string `json:"boot_id"`;ProtocolVersion int `json:"protocol_version"`;Capabilities []string `json:"capabilities"`}
 if err:=strictStorageAgentData(m.Data,&x);err!=nil{return nil,err}
 if x.StorageNodeID!=caller.DeviceID||x.ProtocolVersion!=1{return nil,errors.New("STORAGE_NODE_ID_MISMATCH")}
 e,err:=s.storageAgents.Register(x.StorageNodeID,x.BootID,caller.SessionID,x.Capabilities);if err!=nil{return nil,err}
 return map[string]any{"storage_node_id":e.NodeID,"session_epoch":e.SessionEpoch,"status":"registered","heartbeat_interval_seconds":20},nil
}
func(s *Server)storageAgentHeartbeat(_ context.Context,m commands.Inbound,c commands.ClientContext)(any,error){
 if c.ClientType!="storage_service"{return nil,errors.New("STORAGE_AGENT_CLIENT_REQUIRED")}
 var x struct{StorageNodeID string `json:"storage_node_id"`;SessionEpoch uint64 `json:"session_epoch"`}
 if err:=strictStorageAgentData(m.Data,&x);err!=nil{return nil,err};if x.StorageNodeID!=c.DeviceID{return nil,errors.New("STORAGE_NODE_ID_MISMATCH")}
 if err:=s.storageAgents.Heartbeat(c.DeviceID,c.SessionID,x.SessionEpoch);err!=nil{return nil,err}
 return map[string]any{"status":"alive"},nil
}
// Inventory publishes discovered private file WS endpoints. They are
// addresses, not authentication credentials. Backend forwards metadata only;
// file operations stay directly between HomeCloudUI and HC Storage Service.
func validateStorageFileWS(raw json.RawMessage) error {
 if len(raw)==0 || string(raw)=="null" {return nil}
 var published struct {
  Endpoint string `json:"endpoint"`
  Endpoints []string `json:"endpoints,omitempty"`
  Mode string `json:"mode,omitempty"` // legacy display field, not an auth switch
 }
 if err:=strictStorageAgentData(raw,&published);err!=nil{return err}
 if published.Mode!="" && published.Mode!="trusted_network" {
  return errors.New("INVALID_FILE_WS_MODE")
 }
 if published.Endpoint=="" || len(published.Endpoints)>16 {
  return errors.New("INVALID_FILE_WS_ENDPOINTS")
 }
 if err:=validateStorageFileEndpoint(published.Endpoint);err!=nil{return err}
 if len(published.Endpoints)==0 {return nil} // original HC SS advertises one
 if published.Endpoints[0]!=published.Endpoint {
  return errors.New("FILE_WS_PRIMARY_MISMATCH")
 }
 seen:=make(map[string]bool,len(published.Endpoints))
 for _, endpoint:=range published.Endpoints {
  if seen[endpoint] {return errors.New("FILE_WS_DUPLICATE_ENDPOINT")}
  seen[endpoint]=true
  if err:=validateStorageFileEndpoint(endpoint);err!=nil{return err}
 }
 return nil
}

func validateStorageFileEndpoint(endpoint string) error {
 parsed,err:=url.Parse(endpoint)
 if err!=nil || parsed.Scheme!="ws" || parsed.Path!="/ws/files" ||
    parsed.User!=nil || parsed.RawQuery!="" || parsed.Fragment!="" ||
    parsed.Opaque!="" {return errors.New("INVALID_FILE_WS_ENDPOINT")}
 ip,err:=netip.ParseAddr(parsed.Hostname())
 port,portErr:=strconv.Atoi(parsed.Port())
 tailnet:=netip.MustParsePrefix("100.64.0.0/10")
 if err!=nil||portErr!=nil||port<1||port>65535||!ip.Is4()||
    !(ip.IsPrivate()||ip.IsLoopback()||tailnet.Contains(ip)) {
   return errors.New("FILE_WS_PRIVATE_IP_REQUIRED")
 }
 return nil
}
func(s *Server)storageAgentInventory(_ context.Context,m commands.Inbound,c commands.ClientContext)(any,error){
 if c.ClientType!="storage_service"{return nil,errors.New("STORAGE_AGENT_CLIENT_REQUIRED")}
 var x struct{StorageNodeID string `json:"storage_node_id"`;SessionEpoch uint64 `json:"session_epoch"`;Volumes json.RawMessage `json:"volumes"`;Roots json.RawMessage `json:"roots"`;FileWS json.RawMessage `json:"file_ws,omitempty"`}
 if err:=strictStorageAgentData(m.Data,&x);err!=nil{return nil,err};if x.StorageNodeID!=c.DeviceID||!json.Valid(x.Volumes)||!json.Valid(x.Roots){return nil,errors.New("INVALID_STORAGE_INVENTORY")}
 if err:=validateStorageFileWS(x.FileWS);err!=nil{return nil,err}
 if err:=s.storageAgents.Inventory(c.DeviceID,c.SessionID,x.SessionEpoch,x.Volumes,x.Roots,x.FileWS);err!=nil{return nil,err}
 return map[string]any{"status":"synced"},nil
}
func(s *Server)storageAgentReply(_ context.Context,m commands.Inbound,c commands.ClientContext)(any,error){
 if c.ClientType!="storage_service"{return nil,errors.New("STORAGE_AGENT_CLIENT_REQUIRED")}
 var x struct{RequestID string `json:"request_id"`;SessionEpoch uint64 `json:"session_epoch"`;Success bool `json:"success"`;Data json.RawMessage `json:"data"`;Error string `json:"error"`}
 if err:=strictStorageAgentData(m.Data,&x);err!=nil{return nil,err};if x.RequestID==""{return nil,errors.New("MISSING_REQUEST_ID")}
 if err:=s.storageAgents.Reply(c.DeviceID,c.SessionID,x.SessionEpoch,x.RequestID,x.Success,x.Data,x.Error);err!=nil{return nil,err}
 return map[string]any{"status":"accepted"},nil
}
func(s *Server)storageAgentStatus(_ context.Context,_ commands.Inbound,_ commands.ClientContext)(any,error){return map[string]any{"agents":s.storageAgents.Status()},nil}
func(s *Server)forwardStorageAgent(ctx context.Context,m commands.Inbound,c commands.ClientContext,action string)(any,error){
 if c.ClientType=="storage_service"{return nil,errors.New("STORAGE_AGENT_CANNOT_RELAY")}
 var request struct{StorageNodeID string `json:"storage_node_id"`;Root json.RawMessage `json:"root,omitempty"`;Path string `json:"path,omitempty"`;Limit int `json:"limit,omitempty"`;Cursor string `json:"cursor,omitempty"`}
 if err:=strictStorageAgentData(m.Data,&request);err!=nil{return nil,err};if request.StorageNodeID==""{return nil,errors.New("STORAGE_NODE_ID_REQUIRED")}
 var data any=map[string]any{}
 switch action {
 case "storage.fs.list","storage.fs.stat":
  if len(request.Root)==0||!json.Valid(request.Root){return nil,errors.New("ROOT_REQUIRED")}
  var root struct{Kind string `json:"kind"`;ID string `json:"id"`}
  if err:=strictStorageAgentData(request.Root,&root);err!=nil{return nil,err}
  if root.ID==""||(root.Kind!="disk"&&root.Kind!="folder"){return nil,errors.New("INVALID_ROOT_SELECTOR")}
  if action=="storage.fs.list"{
   if request.Cursor!="" {
    offset,err:=strconv.Atoi(request.Cursor)
    if err!=nil || offset<0 || offset>20000{return nil,errors.New("INVALID_STORAGE_CURSOR")}
   }
   data=map[string]any{"root":root,"path":request.Path,"limit":request.Limit,"cursor":request.Cursor}
  }else{data=map[string]any{"root":root,"path":request.Path}}
 }
 requestID,epoch,ch,err:=s.storageAgents.NewRequest(request.StorageNodeID);if err!=nil{return nil,err}
 defer s.storageAgents.Cancel(requestID)
 requestCtx,cancel:=context.WithTimeout(ctx,15*time.Second);defer cancel()
 event:=commands.Outbound{V:1,Type:"event",Topic:"storage.agent.request",Data:map[string]any{"request_id":requestID,"session_epoch":epoch,"action":action,"data":data}}
 if err:=s.clients.QueueToDevice(requestCtx,request.StorageNodeID,event);err!=nil{return nil,err}
 payload,err:=storageagent.Wait(requestCtx,ch);if err!=nil{return nil,err}
 if len(payload)==0{return map[string]any{},nil}
 var result any;if err:=json.Unmarshal(payload,&result);err!=nil{return nil,err};return result,nil
}
