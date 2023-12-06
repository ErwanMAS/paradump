create view client_report as 
       select c.*,count(t.ticketid) as cnt from client_info c left join client_activity t on  c.id=t.clientid group by id , email , status , insert_ts , update_ts  ; 
