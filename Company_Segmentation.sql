-- ########### C2 Referred Members ####################
with c2_2021 as (
	select *
	from "ANALYTICS"."LOOKER"."MEMBER_ACQUISITION_FACTS"
	where enrolled_flag = 1
	and traffic_source = 'referral'
	and campaign_id = 'referral_raf'
	and enrollment_date >= '2021-08-01'
	and enrollment_date < current_date
)

-- ########### Daily Actives ####################

, c1_2021 as (
	select distinct s.user_id, M.enrollment_date, min(S.timestamp) as first_login
	from segment.chime_prod.screens S
	join "ANALYTICS"."LOOKER"."MEMBER_ACQUISITION_FACTS" M on M.user_id = S.user_id::varchar and M.enrolled_flag = 1
	join "ANALYTICS"."LOOKER"."USERS_VIEW" U on U.id::varchar = S.user_id::varchar and U.status = 'active'
	where S.timestamp >= '2021-08-01'              -- dateadd(month, -6,current_date)
	and S.timestamp < '2021-09-01'
	and enrollment_date <= dateadd(day, -45,current_date)
	and name = 'Home'
	group by 1,2

)

-- ########### Top of Wallet ####################

, user_transactions as (
	select user_id
	, transaction_month
	, stated_income
	, count(distinct MERCHANT_CATEGORY_CODE) AS m_MCC_code
	from analytics.looker.transactions t
	left join "MYSQL_DB"."CHIME_PROD"."USERS" u on t.user_id = u.id
	WHERE PURCHASE_DOLLARS > 0
	and transaction_month >= '2021-07-01'    -- Date needs to be 1month back
	and transaction_month < '2021-08-01'
	GROUP BY 1,2,3
	having count(distinct MERCHANT_CATEGORY_CODE) >= 8
)

, min_top_wallet_month as (
	select user_id, min(transaction_month) as min_month
	from user_transactions
	group by 1
)

, tow_users_2021 as (
	select m.user_id, m.min_month, count(distinct t.transaction_month) as tow_months
	from min_top_wallet_month m
	inner join user_transactions t on t.user_id::varchar = m.user_id::varchar
	group by 1,2
)


-- ########### Clicked Invite Users ####################

, cta_button as (
	select distinct user_id, id, timestamp
	from "SEGMENT"."CHIME_PROD"."CTA_BUTTON_TAPPED" T
	where unique_id in ('invite_friends_invite_button_next_to_contact', 'invite_friends', 'invite_friends_action_panel_ok_button')
	and T.timestamp >= '2021-08-01'
	and T.timestamp < current_date
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



select case when dd1.user_id is null then 0 else 1 end as C1_30DD,
	   case when datediff(day, c1.enrollment_date, c1.first_login) > 14 then 0 else 1 end as new_member,
	   case when tow.user_id is null then 0 else 1 end as TOW,
	   case when left(dd1.company_name,4) in ('WAL-','AMAZ','Door','DOLL','HOME','UIA ','FL D','NYS ','LOWE','DAIL','MPLS','GUST','CIRC','TWC-','AMAZ') then 1 else 0 end as like_walmart_company,
		count(distinct c1.user_id) as Users,
		count(distinct t.user_id) as invited,
		count(distinct c2.referred_by) as referred,
		count(distinct c2.user_id) as referrals,
		count(distinct dd2.user_id) as referrals_30dd
from c1_2021 c1
left join dd_2021 dd1 on dd1.user_id::varchar = c1.user_id::varchar and datediff(day, c1.enrollment_date, dd1.TRANSACTION_TIMESTAMP) between 0 and 30
left join tow_users_2021 tow on tow.user_id::varchar = c1.user_id::varchar 
left join cta_button T on T.user_id::varchar = C1.user_id::varchar   and datediff(day, c1.first_login, T.timestamp) between 0 and 30                    --T.timestamp >= c1.enrollment_date
left join c2_2021 c2 on c1.user_id::varchar = c2.referred_by::varchar and datediff(day, c1.enrollment_date, c2.enrollment_date) between 0 and 30             -- c2.enrollment_date >= c1.enrollment_date
left join dd_2021 dd2 on dd2.user_id::varchar = c2.user_id::varchar and datediff(day, c2.enrollment_date, dd2.TRANSACTION_TIMESTAMP) between 0 and 30
group by 1,2,3,4
order by 2,3,4,5 desc