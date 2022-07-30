Задание 1
1. Требования к витрине:
располагаться в той же базе в схеме analysis, 
данные с начала 2021 года, 
обновления не нужны.
Название dm_rfm_segments

Необходимая структура:
Витрина должна состоять из таких полей:
	⁃	user_id
	⁃	recency (число от 1 до 5)
	⁃	frequency (число от 1 до 5)
	⁃	monetary_value (число от 1 до 5)

Для анализа нужно отобрать только успешно выполненные заказы - Это заказ со статусом Closed


2.
Источники чтобы построить витрину:
	•	user_id  - из таблицы production.orders  поле user_id, orderstatuses
	•	recency - из таблицы production.orders  поле user_id, orderstatuses, order_ts
	•	Frequency - из таблицы production.orders  поле user_id,  orderstatuses, order_id
	•	monetary_value -  из таблицы production.orders  поле user_id,  orderstatuses, payment

3.
Проверка качества данных в production.orders: 
	•	orders_id - дублей нет
	•	Пропущенных значений нет
	•	В таблице orders все поля имеют корректные типы данных
4.1. 
Подготовьте витрину данных

create view analysis.v_orderitems as
select * from production.orderitems;

create view analysis.v_orders as
select * from production.orders;

create view analysis.v_orderstatuses as
select * from production.orderstatuses;

create view analysis.v_orderstatuslog as
select * from production.orderstatuslog;

create view analysis.v_products as
select * from production.products; 

create view analysis.v_users as
select * from production.users; 


4.2 Теперь нужно заполнить витрину
CREATE table IF NOT EXISTS  analysis.dm_rfm_segments 
(
  user_id integer not null,
  recency integer CHECK (recency BETWEEN 1 AND 5) not null,
  frequency integer CHECK (frequency BETWEEN 1 AND 5) not null,
  monetary_value integer CHECK (monetary_value BETWEEN 1 AND 5) not null
);
insert into analysis.dm_rfm_segments (
user_id, 
  recency,
  frequency,
  monetary_value
)
with recency_ex as (
select 
user_id,
max(order_ts),
EXTRACT(DAY from now()-max(order_ts)) AS DateDifference
from analysis.v_orders inner join analysis.v_orderstatuslog on (v_orders.order_id = v_orderstatuslog.order_id)
where status_id in (1, 4) -- Closed or Open
group by 1
order by 1 desc), 
recency as (
select user_id,
  case when row_n < 200 then 1
	when row_n between 200 and 399 then 2
	when row_n between 400 and 599 then 3
	when row_n between 600 and 799 then 4
	else 5 end as recency
from
(
select *,
ROW_NUMBER () OVER (
      order BY datedifference) as row_n
  from recency_ex) t),
  frequency_ex as 
  (
  select
 user_id,
count(v_orders.order_id) as cnt_order
from analysis.v_orders inner join analysis.v_orderstatuslog on (v_orders.order_id = v_orderstatuslog.order_id)
where status_id in (1, 4)-- Closed or Open
group by 1
  ),
   frequency as (
select user_id,
  case when row_n_fr < 200 then 1
	when row_n_fr between 200 and 399 then 2
	when row_n_fr between 400 and 599 then 3
	when row_n_fr between 600 and 799 then 4
	else 5 end as frequency
from
(
select *,
ROW_NUMBER () OVER (
      order BY cnt_order) as row_n_fr
  from frequency_ex) f),
   monetary_ex as 
   (
   select
 user_id,
sum(payment) as total
from analysis.v_orders inner join analysis.v_orderstatuslog on (v_orders.order_id = v_orderstatuslog.order_id)
where status_id in (1, 4) -- Closed or Open
group by 1
   ), 
   monetary as (
select user_id,
  case when row_n_mon < 200 then 1
	when row_n_mon between 200 and 399 then 2
	when row_n_mon between 400 and 599 then 3
	when row_n_mon between 600 and 799 then 4
	else 5 end as monetary
from
(
select *,
ROW_NUMBER () OVER (
      order BY total) as row_n_mon
  from monetary_ex) m)
  select r.user_id, 
  recency,
  frequency,
  monetary
  from recency r
 inner join frequency f on (r.user_id = f.user_id)
 inner join monetary m on (r.user_id = m.user_id);


Задание 2.
CREATE OR REPLACE VIEW analysis.v_orders
AS
select 
o.order_id, 
o.order_ts,
o.user_id, 
o.bonus_payment,
o.payment,
o."cost",
o.bonus_grant, 
x.status_id
 from
 production.orders o
 left join 
 (select order_id, status_id
 from 
 (
 select order_id, status_id,dttm,
 row_number  () OVER (
 	partition by order_id
      order BY dttm desc) as row_dt
 from production.orderstatuslog )r
where row_dt = 1
 ) as x on (o.order_id = x.order_id)
