begin transaction;
create table if not exists requests as 
select 
 min(q.time) firstRequest,
 min(r.time) firstAnswer,
 r.clientId clientId,
 r.reqId reqId
from clientReply r inner join clientrequest q using (clientId, reqId)
group by r.clientId, r.reqId ;

create table requestStats_temp as
select 
 min(r.firstRequest) request,
 min(p.time) proposed,
 min(d.time) decided,
 min(s.time) startExec,
 min(e.time) endExec,
 min(r.firstAnswer) answer
from requests r
join startexecution s on r.clientId = s.clientId and r.reqId = s.reqId
join endexecution e   on r.clientId = e.clientId and r.reqId = e.reqId
join instreq i        on r.clientId = i.clientId and r.reqId = i.reqId
join proposing p      on i.instanceId = p.instanceId
join decided d        on i.instanceId = d.instanceId
group by r.clientId, r.reqId;

create table requestStats as
select 
 request               requ,
 proposed  - request   prop,
 decided   - request   deci,
 startExec - request   stEx,
 endExec   - request   enEx,
 answer    - request   answ
from requestStats_temp;

drop table requestStats_temp;
commit;