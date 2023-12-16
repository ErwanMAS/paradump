create view $DB.client_report as
       select c.*,count(t.ticketid) as cnt from $DB.client_info c left join $DB.client_activity t on  c.id=t.clientid group by id , email , status , insert_ts , update_ts  ;
