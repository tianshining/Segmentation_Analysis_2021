-- ########### C2 Referred Members ####################
with c2_2021 as (
	select *
	from "ANALYTICS"."LOOKER"."MEMBER_ACQUISITION_FACTS"
	where enrolled_flag = 1
	and traffic_source = 'referral'
	and campaign_id = 'referral_raf'
	and enrollment_date >= '2021-08-01'
	and enrollment_date < '2021-09-01'
)



, cta_button as (
	select distinct user_id, id, timestamp
	from "SEGMENT"."CHIME_PROD"."CTA_BUTTON_TAPPED" T
	where unique_id in ('invite_friends_invite_button_next_to_contact', 'invite_friends', 'invite_friends_action_panel_ok_button')
	and T.timestamp >= '2021-08-01'
	and T.timestamp < '2021-09-01'
)


-- ########### 30 Day DD-ed ####################

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

select 
    C1_30DD,
		count(distinct c1.user_id) as Users,
		count(distinct T.user_id) as invited,
		count(distinct c2.referred_by) as referred,
		count(distinct c2.user_id) as referrals,
		count(distinct dd2.user_id) as referrals_30dd
from analytics.test.tning_user_referral_facts c1
left join cta_button T on T.user_id::varchar = C1.user_id::varchar and date_trunc(month, c1.login_month::date) = date_trunc(month, T.timestamp::date)
left join c2_2021 c2 on c1.user_id::varchar = c2.referred_by::varchar and c1.enrollment_date <= c2.enrollment_date and c1.login_month::date = date_trunc(month,c2.enrollment_date::date)
left join dd_2021 dd2 on dd2.user_id::varchar = c2.user_id::varchar and datediff(day, c2.enrollment_date, dd2.TRANSACTION_TIMESTAMP) between 0 and 30
group by 1