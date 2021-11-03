CREATE OR REPLACE TABLE analytics.test.tning_user_referral_facts as


with c1_2021 as (
	select distinct s.user_id, 
					M.enrollment_date, 
					TRAFFIC_SOURCE, 
					CAMPAIGN_ID,
					case when STATED_INCOME <0 then 0 else STATED_INCOME end as STATED_INCOME, 
					date_trunc(month, S.timestamp::date) as login_month, 
					min(S.timestamp) as first_login
	from segment.chime_prod.screens S
	join "ANALYTICS"."LOOKER"."MEMBER_ACQUISITION_FACTS" M on M.user_id = S.user_id::varchar and M.enrolled_flag = 1
	join "ANALYTICS"."LOOKER"."USERS_VIEW" U on U.id::varchar = S.user_id::varchar and U.status = 'active'
	where S.timestamp >= '2021-08-01'              -- dateadd(month, -6,current_date)
	and S.timestamp < '2021-09-01'
	and name = 'Home'
	group by 1,2,3,4,5,6
)

-- ########### Transact ####################

, active_purchase as (
	select user_id
	, date_trunc(month, transaction_month::date) as transaction_month
	, count(distinct id) as num_transactions
	, avg(PURCHASE_DOLLARS) as avg_purchase
	, count(distinct MERCHANT_CATEGORY_CODE) as num_mcc
	from analytics.looker.transactions t
	WHERE PURCHASE_DOLLARS > 0
	and transaction_month >= '2021-08-01'  
	and transaction_month < '2021-09-01'
    group by 1,2
)

-- ########### Spot Me Activated ####################

, active_spotme as (
	select distinct user_id, first_enrolled_at, limit, MOST_NEGATIVE_AMOUNT
	from analytics.looker.spotme_limit_facts
	where status = 'enrolled'
	and first_enrolled_at >= '2015-01-01'
)


-- ########### Active Boost ####################

, active_boost_sender as (
	select user_id, date_trunc(month, timestamp::date) as boost_month, count(distinct id) as num_boosts, sum(amount) as boost_amount
	from analytics.test.spotme_boosts
	where status = 'Success'
	and timestamp >= '2021-08-01'
	and timestamp < '2021-09-01'
	group by 1,2
)

, active_boost_receiver as (
	select RECEIVER_ID as user_id, date_trunc(month, timestamp::date) as boost_month, count(distinct id) as num_boosts, sum(amount) as boost_amount
	from analytics.test.spotme_boosts
	where status = 'Success'
	and timestamp >= '2021-08-01'
	and timestamp < '2021-09-01'
	group by 1,2
)


-- ########### Credit Builder Activated ####################

, credit_builder_activated as (
	SELECT a.user_id, MIN(a.event_at) as activated_at
	FROM mysql_db.chime_prod.account_events a
	JOIN mysql_db.galileo.galileo_account_cards g ON a.card_id = g.card_id
	AND a.type = 'card_activated'
	AND g.unique_program_id in ('600', '278', '1014')
	GROUP BY 1
)



-- ########### Sent 1st Pay Friend ####################

, pay_friends_sender as (
	select member_id as user_id, date_trunc(month, created_at::date) as pf_month, count(PAY_FRIENDS_ID) as pf_transactions, sum(AMOUNT) as pf_amount
	from analytics.looker.pay_friends_transactions_view
	where sender_or_receiver = 'sender'
	and pf_type = 'Pay Friends'
	and created_at >= '2021-08-01'
	and created_at < '2021-09-01'
	group by 1,2
)

, pay_friends_receiver as (
	select member_id as user_id, date_trunc(month, created_at::date) as pf_month, count(PAY_FRIENDS_ID) as pf_transactions, sum(AMOUNT) as pf_amount
	from analytics.looker.pay_friends_transactions_view
	where sender_or_receiver = 'receiver'
	and pf_type = 'Pay Friends'
	and created_at >= '2021-08-01'
	and created_at < '2021-09-01'
	group by 1,2
)

-- ########### Savings Activated ####################

, savings_activated as (
	select user_id, min(ACCOUNT_CREATED_TS) as savings_activated_date, sum(available_balance) as savings_available_balance
	from edw_db.core.dim_accounts
	where account_type in ('savings')
	and account_status = 'active'
	group by 1
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

, active_dd as (
	select distinct user_id, month as dd_month
	from analytics.test.tj_monthly_active_dders
	where month >= '2015-01-01'
	and month < current_date
	and active = 1
	and dder = 1
)



select distinct c1.login_month, 
				c1.user_id,
				c1.stated_income,
				c1.enrollment_date,
				case when c1.TRAFFIC_SOURCE = 'referral' and c1.CAMPAIGN_ID = 'referral_raf' then 'referral'
					 when c1.TRAFFIC_SOURCE = 'referral' and c1.CAMPAIGN_ID = 'referral_pa2' then 'pay_anyone'
					 else c1.TRAFFIC_SOURCE end as traffic,
				case when datediff(day, c1.enrollment_date, dd1.TRANSACTION_TIMESTAMP) between 0 and 30 then 1 else 0 end as C1_30DD,
				case when dd1.user_id is not null then 1 else 0 end as C1_DD_Ever,
				case when add1.user_id is not null then 1 else 0 end as C1__Active_DD,
				case when datediff(day, c1.enrollment_date, c1.login_month) between 0 and 30 then 1 else 0 end as member_new, 
        		case when datediff(day, c1.enrollment_date, c1.login_month) >= 90 then 1 else 0 end as member_90days, 
        		case when ut.user_id is null then 0 else 1 end as Transact_Active,
        		case when left(dd1.company_name,4) in ('WAL-','AMAZ','Door','DOLL','HOME','UIA ','FL D','NYS ','LOWE','DAIL','MPLS','GUST','CIRC','TWC-','AMAZ') then 1 else 0 end as like_walmart_company,
		        case when sp.user_id is not null then 1 end as spot_me_enrolled,
		   	   	case when bs.user_id is not null then 1 end as boost_sender,
		   	   	case when br.user_id is not null then 1 end as boost_receiver,
		   	    case when cb.user_id is not null then 1 end as credit_builder_enrolled,
		   	    case when pfs.user_id is not null then 1 end as pay_friend_sender,
		   	    case when pfr.user_id is not null then 1 end as pay_friend_receiver,
		   	   	case when svg.user_id is not null then 1 end as savings_activated
from c1_2021 c1
left join dd_2021 dd1 on dd1.user_id::varchar = c1.user_id::varchar and c1.enrollment_date <= dd1.TRANSACTION_TIMESTAMP
left join active_dd add1 on add1.user_id::varchar = c1.user_id::varchar and add1.dd_month = c1.login_month
left join active_purchase ut on ut.user_id::varchar = c1.user_id::varchar and ut.transaction_month = c1.login_month


-- ######## Product use join ###################

-- ########  On Activation ######## 
left join active_spotme sp on sp.user_id::varchar = c1.user_id::varchar and sp.first_enrolled_at >= c1.enrollment_date
left join credit_builder_activated cb on cb.user_id::varchar = c1.user_id::varchar and cb.activated_at >= c1.enrollment_date 
left join savings_activated svg on svg.user_id::varchar = c1.user_id::varchar and svg.savings_activated_date >= c1.enrollment_date 

-- ########  On Usage ######## 
left join active_boost_sender bs on bs.user_id::varchar = c1.user_id::varchar and bs.boost_month >= c1.enrollment_date and bs.boost_month = c1.login_month
left join active_boost_receiver br on br.user_id::varchar = c1.user_id::varchar and br.boost_month >= c1.enrollment_date and br.boost_month = c1.login_month
left join pay_friends_sender pfs on pfs.user_id::varchar = c1.user_id::varchar and pfs.pf_month >= c1.enrollment_date and pfs.pf_month = c1.login_month
left join pay_friends_receiver pfr on pfr.user_id::varchar = c1.user_id::varchar and pfr.pf_month >= c1.enrollment_date and pfr.pf_month = c1.login_month