package storageagent
import("context";"encoding/json";"errors";"testing")
func TestRegistrationAndSessionFence(t *testing.T){r:=New();e,err:=r.Register("node","boot",1,[]string{"fs_list"});if err!=nil||e.SessionEpoch==0{t.Fatal(e,err)}
 if _,err:=r.Register("node","other",2,nil);!errors.Is(err,ErrDuplicate){t.Fatal(err)}
 if err:=r.Heartbeat("node",2,e.SessionEpoch);!errors.Is(err,ErrWrongSession){t.Fatal(err)}
 if err:=r.Heartbeat("node",1,e.SessionEpoch);err!=nil{t.Fatal(err)}
 req,ep,ch,err:=r.NewRequest("node");if err!=nil||ep!=e.SessionEpoch{t.Fatal(err)}
 if err:=r.Reply("node",2,ep,req,true,json.RawMessage(`{"ok":true}`),"");!errors.Is(err,ErrWrongSession){t.Fatal(err)}
 if err:=r.Reply("node",1,ep,req,true,json.RawMessage(`{"ok":true}`),"");err!=nil{t.Fatal(err)}
 b,err:=Wait(context.Background(),ch);if err!=nil||string(b)!=`{"ok":true}`{t.Fatal(err,string(b))}
 r.Disconnect("node",2);if len(r.Status())!=1{t.Fatal("wrong session removed")}
 r.Disconnect("node",1);if len(r.Status())!=0{t.Fatal("not removed")}
}
func TestOfflineCancelsPending(t *testing.T){r:=New();e,_:=r.Register("disk","boot",3,nil);id,_,ch,_:=r.NewRequest("disk");r.Disconnect("disk",3)
 if _,err:=Wait(context.Background(),ch);!errors.Is(err,ErrAgentStale){t.Fatal(err)}
 if err:=r.Reply("disk",3,e.SessionEpoch,id,true,nil,"");!errors.Is(err,ErrNotFound){t.Fatal(err)}
}
