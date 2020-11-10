drop table if exists rising.myv_universe_progress;
create table rising.myv_universe_progress as (

#here is my change!!!!!
#change number 2
with texts as (
select vanid, resultshortname, datecanvassed, statecode as vb_vf_source_state from (
select cc.datecanvassed
, cc.vanid
, r.resultshortname
, r.resultid 
, cc.contactscontactid
, statecode
, row_number() over (partition by vanid order by datecanvassed desc, resultshortname asc) as row
from van.tsm_nextgen_contactscontacts_vf cc
left join van.tsm_nextgen_results r using(resultid)
left join van.tsm_nextgen_contacttypes c using(contacttypeid)
where date(cc.datecanvassed) >= date('2020-09-11')
and c.contacttypename = 'SMS Text'
) 

union


select b.vb_smartvan_id as vanid
, case when s.response = 'Yes' and s.survey_question = 'Do you Pledge to Vote for Joe Biden?' then 'Biden ID'
  	when s.response is not null and not (response = 'Yes' and survey_question = 'Do you Pledge to Vote for Joe Biden?') then 'Canvassed'
  	else null end as resultshortname
, date(s.updated_at) as datecanvassed
, b.vb_vf_source_state 
from thrutext.messages m 
left join thrutext.messages_xf mxf on m.message_id = mxf.message_id
left join thrutext.messages_xf o ON (o.col_name = 'voterbase_id' AND o.message_id = mxf.message_id)
left join thrutext.surveys s on (mxf.col_name = 'contact_id' AND s.contact_id = mxf.col_value) and m.contact_first_name = s.contact_first_name 
left join universes.gotv_myv_current b on o.col_value = b.vb_voterbase_id
where date(ttimestamp) >= date('11-Sep-2020') and split_part(m.import_source, '/', 1) ilike 'NextGen'

)

, calls as (
select vanid, resultshortname, statecode as vb_vf_source_state, datecanvassed from (
select cc.datecanvassed
, cc.vanid
, r.resultshortname
, r.resultid 
, cc.contactscontactid
, statecode
, row_number() over (partition by vanid order by datecanvassed desc, resultshortname asc) as row
from van.tsm_nextgen_contactscontacts_vf cc
left join van.tsm_nextgen_results r using(resultid)
left join van.tsm_nextgen_contacttypes c using(contacttypeid)
where date(cc.datecanvassed) >= date('2020-09-11')
and c.contacttypename = 'Phone'
) 
)

, ids as (  
select  
coalesce(statecode, t.vb_vf_source_state) as vb_vf_source_state
, case when t.resultshortname = 'Biden ID' or csr.surveyresponseid is not null 
  	then 'Y' else null end 
  	as surveyresponseid 
, case when (t.datecanvassed = csr.datecanvassed or t.resultshortname = 'Biden ID') then 'text'
    when c.datecanvassed = csr.datecanvassed then 'call' 
    else null end 
    as method 
, coalesce(csr.vanid, t.vanid) as vanid
, csr.datecanvassed
from texts t
left join van.tsm_nextgen_contactssurveyresponses_vf csr on t.vb_vf_source_state = csr.statecode and t.vanid = csr.vanid
left join calls c on c.vb_vf_source_state = csr.statecode and c.vanid = csr.vanid
where ((csr.surveyquestionid = '369282'
and csr.surveyresponseid in ('1519016', '1519015')) or t.resultshortname = 'Biden ID')
and (date(csr.datecanvassed) >= date('2020-09-11') or date(t.datecanvassed) >= date('2020-09-11'))
and ((t.datecanvassed = csr.datecanvassed or c.datecanvassed = csr.datecanvassed) or t.resultshortname = 'Biden ID')
)

select *, current_timestamp AT TIME ZONE 'PST8DST' as update_time from (
(select 
b.vb_vf_source_state::varchar(100) as vb_vf_source_state
, segment
, b.vb_vf_source_state::varchar(100)|| segment as lookup
, count(distinct b.vb_smartvan_id) as Universe
, count(distinct case when (t.vanid is not null or c.vanid is not null) then b.vb_smartvan_id || b.vb_vf_source_state else null end) as Attempted
, count(distinct case when (t.resultshortname in('Canvassed', 'Biden ID') or c.resultshortname = 'Canvassed') then b.vb_smartvan_id || b.vb_vf_source_state else null end) as Canvassed
, count(distinct t.vanid || t.vb_vf_source_state) as texted
, count(distinct case when t.resultshortname in('Canvassed', 'Biden ID') then t.vanid || t.vb_vf_source_state else null end) as t_canvassed
, count(distinct c.vanid || c.vb_vf_source_state) as called
, count(distinct case when c.resultshortname = 'Canvassed' then c.vanid || c.vb_vf_source_state else null end) as c_canvassed
, count(distinct case when i.surveyresponseid is not null then i.vanid else null end) as ids
, count(distinct case when i.method = 'text' then i.vanid || i.vb_vf_source_state else null end) as t_ids
, count(distinct case when i.method = 'call' then i.vanid || i.vb_vf_source_state else null end) as c_ids
from universes.gotv_myv_current b
left join texts t on t.vb_vf_source_state = b.vb_vf_source_state and t.vanid = b.vb_smartvan_id
left join calls c on c.vb_vf_source_state = b.vb_vf_source_state and c.vanid = b.vb_smartvan_id
left join ids i on i.vb_vf_source_state = b.vb_vf_source_state and i.vanid = b.vb_smartvan_id 
where (b.vb_vf_source_state != 'VA') or (b.vb_vf_cd in (2,5,7) and b.vb_vf_source_state = 'VA' and segment = 'Registered Low Turnout') or (b.vb_vf_source_state = 'VA' and segment != 'Registered Low Turnout')
group by 1, 2 order by 1, 2)

union

(select 
'All States'
, segment
, 'All States' || segment as lookup
, count(distinct b.vb_smartvan_id) as Universe
, count(distinct case when (t.vanid is not null or c.vanid is not null) then b.vb_smartvan_id || b.vb_vf_source_state else null end) as Attempted
, count(distinct case when (t.resultshortname in('Canvassed', 'Biden ID') or c.resultshortname = 'Canvassed') then b.vb_smartvan_id else null end) as Canvassed
, count(distinct t.vanid) as texted
, count(distinct case when t.resultshortname in('Canvassed', 'Biden ID') then t.vanid else null end) as t_canvassed
, count(distinct c.vanid) as called
, count(distinct case when c.resultshortname = 'Canvassed' then c.vanid else null end) as c_canvassed
, count(distinct case when i.surveyresponseid is not null then i.vanid else null end) as ids
, count(distinct case when i.method = 'text' then i.vanid else null end) as t_ids
, count(distinct case when i.method = 'call' then i.vanid else null end) as c_ids
from universes.gotv_myv_current b
left join texts t on t.vb_vf_source_state = b.vb_vf_source_state and t.vanid = b.vb_smartvan_id
left join calls c on c.vb_vf_source_state = b.vb_vf_source_state and c.vanid = b.vb_smartvan_id
left join ids i on i.vb_vf_source_state = b.vb_vf_source_state and i.vanid = b.vb_smartvan_id 
where (b.vb_vf_source_state != 'VA') or (b.vb_vf_cd in (2,5,7) and b.vb_vf_source_state = 'VA' and segment = 'Registered Low Turnout') or (b.vb_vf_source_state = 'VA' and segment != 'Registered Low Turnout')
group by 1, 2 order by 1, 2)
)
order by case when vb_vf_source_state = 'All States' then 0 else 1 end desc, vb_vf_source_state asc, segment
);

--EA UNIVERSE
drop table if exists rising.ea_universe_progress;
create table rising.ea_universe_progress as (


with texts as (
select vanid, resultshortname, datecanvassed, state as state from (
select cc.datecanvassed
, cc.vanid
, r.resultshortname
, r.resultid 
, cc.contactscontactid
, case WHEN cc.committeeid = 56351 THEN 'NV'
              WHEN cc.committeeid = 62658 THEN 'FL'
              WHEN cc.committeeid = 56350 THEN 'IA'
              WHEN cc.committeeid = 56354 THEN 'PA'
              WHEN cc.committeeid = 60315 THEN 'VA'
              WHEN cc.committeeid = 64624 THEN 'AZ'
              WHEN cc.committeeid = 56352 THEN 'NH'
              WHEN cc.committeeid = 57273 THEN 'NC'
              WHEN cc.committeeid = 62659 THEN 'MI'
              WHEN cc.committeeid = 76878 THEN 'ME'
              WHEN cc.committeeid = 62660 THEN 'WI'
              when cc.committeeid = 85292 then 'DIST'
      ELSE 'OTHER' END as state
from van.tsm_nextgen_contactscontacts_mym cc
left join van.tsm_nextgen_results r using(resultid)
left join van.tsm_nextgen_contacttypes c using(contacttypeid)
where date(cc.datecanvassed) >= date('2020-09-11')
and c.contacttypename = 'SMS Text')
)

, calls as (
select vanid, resultshortname, state, datecanvassed from (
select cc.datecanvassed
, cc.vanid
, r.resultshortname
, r.resultid 
, cc.contactscontactid
, case WHEN cc.committeeid = 56351 THEN 'NV'
              WHEN cc.committeeid = 62658 THEN 'FL'
              WHEN cc.committeeid = 56350 THEN 'IA'
              WHEN cc.committeeid = 56354 THEN 'PA'
              WHEN cc.committeeid = 60315 THEN 'VA'
              WHEN cc.committeeid = 64624 THEN 'AZ'
              WHEN cc.committeeid = 56352 THEN 'NH'
              WHEN cc.committeeid = 57273 THEN 'NC'
              WHEN cc.committeeid = 62659 THEN 'MI'
              WHEN cc.committeeid = 76878 THEN 'ME'
              WHEN cc.committeeid = 62660 THEN 'WI'
              when cc.committeeid = 85292 then 'DIST'
      ELSE 'OTHER' END as state
, row_number() over (partition by vanid order by datecanvassed desc, resultshortname asc) as row
from van.tsm_nextgen_contactscontacts_mym cc
left join van.tsm_nextgen_results r using(resultid)
left join van.tsm_nextgen_contacttypes c using(contacttypeid)
where date(cc.datecanvassed) >= date('2020-09-11')
and c.contacttypename = 'Phone') 
)


, ids as (  
select coalesce(c.state, t.state) as state
, case when csr.surveyresponseid is not null 
    then 'Y' else null end 
    as surveyresponseid 
, case when t.datecanvassed = csr.datecanvassed then 'text'
    when c.datecanvassed = csr.datecanvassed then 'call' 
    else null end 
    as method 
, csr.vanid 
, csr.datecanvassed
from texts t
left join van.tsm_nextgen_contactssurveyresponses_mym csr using(vanid)
left join calls c using(vanid)
where csr.surveyquestionid = '369282'
and csr.surveyresponseid in ('1519016', '1519015')
and date(csr.datecanvassed) >= date('2020-09-11') 
and (t.datecanvassed = csr.datecanvassed or c.datecanvassed = csr.datecanvassed)
)

select *, current_timestamp AT TIME ZONE 'PST8DST' as update_time 
from (
(select b.state
, 'EA' as segment
, count(distinct b.vanid || state) as Universe
, count(distinct case when (t.vanid is not null or c.vanid is not null) then b.vanid || b.state else null end) as Attempted
, count(distinct case when (t.resultshortname = 'Canvassed' or c.resultshortname = 'Canvassed') then b.vanid || b.state else null end) as Canvassed
, count(distinct t.vanid || t.state) as texted
, count(distinct case when t.resultshortname  = 'Canvassed' then t.vanid || t.state else null end) as t_canvassed
, count(distinct c.vanid || c.state) as called
, count(distinct case when c.resultshortname = 'Canvassed' then c.vanid || c.state else null end) as c_canvassed
, count(distinct case when i.surveyresponseid is not null then i.vanid || i.state else null end) as ids
, count(distinct case when i.method = 'text' then i.vanid || i.state else null end) as t_ids
, count(distinct case when i.method = 'call' then i.vanid || i.state else null end) as c_ids
from 
 	(select case WHEN cc.committeeid = 56351 THEN 'NV'
              WHEN cc.committeeid = 62658 THEN 'FL'
              WHEN cc.committeeid = 56350 THEN 'IA'
              WHEN cc.committeeid = 56354 THEN 'PA'
              WHEN cc.committeeid = 60315 THEN 'VA'
              WHEN cc.committeeid = 64624 THEN 'AZ'
              WHEN cc.committeeid = 56352 THEN 'NH'
              WHEN cc.committeeid = 57273 THEN 'NC'
              WHEN cc.committeeid = 62659 THEN 'MI'
              WHEN cc.committeeid = 76878 THEN 'ME'
              WHEN cc.committeeid = 62660 THEN 'WI'
			  ELSE 'not in states' END as state
 	, vanid
    from
 	everyaction.daily_target_exports b
	inner join van.tsm_nextgen_contactscontacts_mym cc using(vanid)
 	where b.targetsubgroupname = 'Tier 5: GOTV'
	and date(b.date) = date(current_timestamp at time zone 'PST')
    and state != 'not in states'
 	) b
left join texts t using(vanid, state)
left join calls c using(vanid, state)
left join ids i using(vanid, state)
group by 1, 2 order by 1, 2)

union

(select 
'All States'
, 'EA' as segment
, count(distinct b.vanid || state) as Universe
, count(distinct case when (t.vanid is not null or c.vanid is not null) then b.vanid || b.state else null end) as Attempted
, count(distinct case when (t.resultshortname = 'Canvassed' or c.resultshortname = 'Canvassed') then b.vanid || b.state else null end) as Canvassed
, count(distinct t.vanid || t.state) as texted
, count(distinct case when t.resultshortname  = 'Canvassed' then t.vanid || t.state else null end) as t_canvassed
, count(distinct c.vanid || c.state) as called
, count(distinct case when c.resultshortname = 'Canvassed' then c.vanid || c.state else null end) as c_canvassed
, count(distinct case when i.surveyresponseid is not null then i.vanid || i.state else null end) as ids
, count(distinct case when i.method = 'text' then i.vanid || i.state else null end) as t_ids
, count(distinct case when i.method = 'call' then i.vanid || i.state else null end) as c_ids
from 
 	(select case WHEN cc.committeeid = 56351 THEN 'NV'
              WHEN cc.committeeid = 62658 THEN 'FL'
              WHEN cc.committeeid = 56350 THEN 'IA'
              WHEN cc.committeeid = 56354 THEN 'PA'
              WHEN cc.committeeid = 60315 THEN 'VA'
              WHEN cc.committeeid = 64624 THEN 'AZ'
              WHEN cc.committeeid = 56352 THEN 'NH'
              WHEN cc.committeeid = 57273 THEN 'NC'
              WHEN cc.committeeid = 62659 THEN 'MI'
              WHEN cc.committeeid = 76878 THEN 'ME'
              WHEN cc.committeeid = 62660 THEN 'WI'
              ELSE 'not in states' END as state
 	, vanid
    from
 	everyaction.daily_target_exports b
	inner join van.tsm_nextgen_contactscontacts_mym cc using(vanid)
 	where b.targetsubgroupname = 'Tier 5: GOTV'
	and date(b.date) = date(current_timestamp at time zone 'PST')
    and state != 'not in states'
 	) b
left join texts t using(vanid, state)
left join calls c using(vanid, state)
left join ids i using(vanid, state)
group by 1, 2 order by 1, 2)
)
order by case when state = 'All States' then 0 else 1 end desc, state asc, segment

);
grant all on rising.ea_universe_progress to group state_managers;
grant all on rising.ea_universe_progress to group hq_data;
grant all on rising.myv_universe_progress to group state_managers;
grant all on rising.myv_universe_progress to group hq_data;
