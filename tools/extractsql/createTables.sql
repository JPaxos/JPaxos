begin transaction;

select "request helpers...";

-- helper table for requests #1
create table 
if not exists 
  requests_t1
as
  select 
    min(r.time) firstAnswer,
    r.clientId clientId,
    r.reqId reqId
  from clientReply r
  group by r.clientId, r.reqId
;


-- helper table for requests #2
create table
if not exists
  requests_t2
as
  select
    min(q.time) firstRequest,
    q.clientId clientId,
    q.reqId reqId
  from clientrequest q
  group by q.clientId, q.reqId
;

-- index to make next op way faaaaster
create index
if not exists
  requests_t2_idx
on
  requests_t2
  (clientId, reqId)
;

select "requests..." ;

-- creating the requests table
create table
if not exists
  requests
as
  select
    q.firstRequest firstRequest,
    r.firstAnswer firstAnswer,
    q.clientId clientId,
    q.reqId reqId
  from
    requests_t1 r inner join requests_t2 q
    using (clientId, reqId)
;

-- getting rid of helpers
drop table requests_t1;
drop index requests_t2_idx;
drop table requests_t2;

--
-- prepairing requests table...
--

select "requestStats helper indices ...";

create index if not exists
  startexecution_idx
on 
  startexecution
  (clientId, reqId)
;

create index if not exists
  endexecution_idx
on 
  endexecution
  (clientId, reqId)
;

create index if not exists
  instreq_idx
on 
  instreq
  (clientId, reqId)
;

create index if not exists
  proposing_idx
on 
  proposing
  (instanceId)
;

create index if not exists
  decided_idx
on 
  decided
  (instanceId)
;

select "requestStats helper tables ...";

create table min_startexecution
as
  select min(time) time, clientId, reqId
  from startexecution
  group by clientId, reqId
;

create table min_endexecution
as
  select min(time) time, clientId, reqId
  from endexecution
  group by clientId, reqId
;

create index if not exists
  min_startexecution_idx
on 
  min_startexecution
  (clientId, reqId)
;

create index if not exists
  min_endexecution_idx
on 
  min_endexecution
  (clientId, reqId)
;

select "requestStats_temp...";

create table requestStats_temp as
select
 min(r.firstRequest) request,
 min(p.time) proposed,
 min(d.time) decided,
 s.time startExec,
 e.time endExec,
 min(r.firstAnswer) answer
from requests r
join min_startexecution s on r.clientId = s.clientId and r.reqId = s.reqId
join min_endexecution e   on r.clientId = e.clientId and r.reqId = e.reqId
join instreq i        on r.clientId = i.clientId and r.reqId = i.reqId
join proposing p      on i.instanceId = p.instanceId
join decided d        on i.instanceId = d.instanceId
group by r.clientId, r.reqId;

select "requestStats...";

create table requestStats as
select 
 request               requ,
 proposed  - request   prop,
 decided   - request   deci,
 startExec - request   stEx,
 endExec   - request   enEx,
 answer    - request   answ
from requestStats_temp;

select "cleanup...";

-- dropping helper tables
drop table requestStats_temp;

drop index min_startexecution_idx;
drop index min_endexecution_idx;

drop table min_startexecution;
drop table min_endexecution;

drop index startexecution_idx;
drop index endexecution_idx;
drop index instreq_idx;
drop index proposing_idx;
drop index decided_idx;
commit;
