with c2_2021 as (
	select *
	from "ANALYTICS"."LOOKER"."MEMBER_ACQUISITION_FACTS"
	where enrolled_flag = 1
	and traffic_source = 'referral'
	and campaign_id = 'referral_raf'
	and enrollment_date >= '2021-09-01'
	and enrollment_date < current_date
)

, c1_2021 as (
	select distinct s.user_id, M.enrollment_date, min(S.timestamp) as first_login
	from segment.chime_prod.screens S
	join "ANALYTICS"."LOOKER"."MEMBER_ACQUISITION_FACTS" M on M.user_id = S.user_id::varchar and M.enrolled_flag = 1
	join "ANALYTICS"."LOOKER"."USERS_VIEW" U on U.id::varchar = S.user_id::varchar and U.status = 'active'
	where S.timestamp >= '2021-09-01'              -- dateadd(month, -6,current_date)
	and S.timestamp < '2021-10-01'
	and enrollment_date <= dateadd(day, -45,current_date)
	and name = 'Home'
	group by 1,2

)

, cta_button as (
	select distinct user_id, id, timestamp
	from "SEGMENT"."CHIME_PROD"."CTA_BUTTON_TAPPED" T
	where unique_id in ('invite_friends_invite_button_next_to_contact', 'invite_friends', 'invite_friends_action_panel_ok_button')
	and T.timestamp >= '2021-09-01'
	and T.timestamp < current_date
)



-- , c1_2021 as (	
-- select *
-- from "ANALYTICS"."LOOKER"."MEMBER_ACQUISITION_FACTS"
-- where enrolled_flag = 1
-- and enrollment_date >= '2021-09-01'
-- and enrollment_date < current_date
-- )



, dd_2021 as (
select distinct user_id,
first_value(company_name) over(partition by user_id order by TRANSACTION_TIMESTAMP) as company_name,
first_value(transaction_amount) over(partition by user_id order by TRANSACTION_TIMESTAMP) as transaction_amount,
first_value(DD_type) over(partition by user_id order by TRANSACTION_TIMESTAMP) as DD_type,
first_value(TRANSACTION_TIMESTAMP) over(partition by user_id order by TRANSACTION_TIMESTAMP) as TRANSACTION_TIMESTAMP
from "ANALYTICS"."LOOKER"."QUALIFYING_DIRECT_DEPOSIT_LEDGER" dd
where transaction_timestamp >= '2015-01-01'
and transaction_timestamp < current_date
)



select case when dd1.user_id is null then 0 else 1 end as C1_30DD,
		count(distinct c1.user_id) as Users,
		count(distinct t.user_id) as invited,
		count(distinct c2.referred_by) as referred,
		count(distinct c2.user_id) as referrals,
		count(distinct dd2.user_id) as referrals_30dd
from c1_2021 c1
left join dd_2021 dd1 on dd1.user_id::varchar = c1.user_id::varchar and datediff(day, c1.enrollment_date, dd1.TRANSACTION_TIMESTAMP) between 0 and 30
left join cta_button T on T.user_id::varchar = C1.user_id::varchar   and datediff(day, c1.first_login, T.timestamp) between 0 and 30                    --T.timestamp >= c1.enrollment_date
left join c2_2021 c2 on c1.user_id::varchar = c2.referred_by::varchar and datediff(day, c1.enrollment_date, c2.enrollment_date) between 0 and 30             -- c2.enrollment_date >= c1.enrollment_date
left join dd_2021 dd2 on dd2.user_id::varchar = c2.user_id::varchar and datediff(day, c2.enrollment_date, dd2.TRANSACTION_TIMESTAMP) between 0 and 30
group by 1
order by 2 desc