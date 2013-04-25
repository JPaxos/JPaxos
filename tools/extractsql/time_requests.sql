select load_extension('/home/jasiu/studia/jpaxos/tools/libsqlitefunctions.so');

begin transaction;

-- create table if not exists requests as 
-- select 
--  (select min(q.time) from clientrequest q where q.clientId = r.clientId and q.reqid = r.reqid ) firstRequest,
--  r.time answer,
--  r.clientId,
--  r.reqId
-- from clientReply r;

select 
 (firstRequest/100)/10.0 t,
 avg(firstAnswer - firstRequest) "Latency [ms]",
 stdev(firstAnswer - firstRequest) "Latency stdev [ms]",
 count(*)*10 "Throughput [r/s]"
from requests
where t>=5
group by t
order by t;

commit;