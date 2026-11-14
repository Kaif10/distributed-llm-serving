"""Bounded exhaustive linearizability checking for the config CAS/read/retry API.

503/504/transport-error writes are incomplete operations: they may be omitted or
take effect later, including after the error response. Successful/409 operations
respect their actual return times. This is a finite-history checker, not a proof.
"""
import copy
import json


def check_history(history, max_states=200000):
    if len(history)>24:
        raise ValueError("split the experiment into independently initialized histories of at most 24 operations")
    operations=[]
    horizon=max((op["return_ns"] for op in history),default=0)+1
    for op in history:
        if op["return_ns"]<op["call_ns"]:
            raise ValueError("operation returned before invocation")
        if op["path"]!="/config" or op["method"] not in ("GET","PUT"):
            raise ValueError("checker models only GET/PUT /config")
        if op["status"] not in (200,409,503,504,None):
            raise ValueError("unsupported response classification")
        if op["method"]=="GET" and op["status"]==409:
            raise ValueError("read cannot return a CAS conflict")
        item=copy.deepcopy(op)
        item["unknown"]=op["status"] in (503,504,None)
        item["effective_end"]=horizon if item["unknown"] else item["return_ns"]
        operations.append(item)
    predecessors=[]
    for op in operations:
        mask=0
        for i,other in enumerate(operations):
            if other["effective_end"]<op["call_ns"]:
                mask|=1<<i
        predecessors.append(mask)
    visited=set();explored=0;complete=(1<<len(operations))-1

    def step(state,op):
        if op["method"]=="GET":
            if op["unknown"]:
                return state
            return state if op["response"]==state["current"] else None
        update=op["request"]
        previous=state["requests"].get(update["request_id"])
        next_state=copy.deepcopy(state)
        if previous is not None:
            if previous["update"]==update:
                code,response=200,previous["result"]
            else:
                code,response=409,None
        elif update["expected_revision"]!=state["current"]["revision"]:
            code,response=409,None
        else:
            code=200
            response={"revision":state["current"]["revision"]+1,"workers":update["workers"]}
            next_state["current"]=response
            next_state["requests"][update["request_id"]]={"update":update,"result":response}
        if op["unknown"]:
            return next_state if code==200 else state
        if code!=op["status"] or (code==200 and response!=op["response"]):
            return None
        return next_state

    def search(mask,state):
        nonlocal explored
        if mask==complete:
            return []
        key=(mask,json.dumps(state,sort_keys=True,separators=(",",":")))
        if key in visited:
            return None
        visited.add(key);explored+=1
        if explored>max_states:
            raise RuntimeError("history search budget exhausted; inconclusive")
        for i,op in enumerate(operations):
            bit=1<<i
            if mask&bit or predecessors[i]&~mask:
                continue
            if op["unknown"]:
                # Pending writes may never have taken effect. Dropping one does
                # not assert that it was definitively rejected by the system.
                suffix=search(mask|bit,state)
                if suffix is not None:
                    return [{"id":op["id"],"effect":"omitted incomplete"},*suffix]
            next_state=step(state,op)
            if next_state is None:
                continue
            suffix=search(mask|bit,next_state)
            if suffix is not None:
                return [{"id":op["id"],"effect":"applied"},*suffix]
        return None

    witness=search(0,{"current":{"revision":0,"workers":[]},"requests":{}})
    return {"linearizable":witness is not None,"explored_states":explored,"operations":len(history),"witness":witness}
