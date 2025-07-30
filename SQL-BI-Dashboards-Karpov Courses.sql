--  Задание 1 - Динамика пользователей и курьеров

SELECT date, new_users, new_couriers, 
       sum(new_users) over(order by date)::INTEGER as total_users, 
       sum(new_couriers) over(order by date)::INTEGER as total_couriers 
FROM 
(
  SELECT time_courier as date, new_users, new_couriers 
  FROM 
    (SELECT time_user, count(time_user) as new_users 
     FROM 
       (SELECT user_id, time::date as time_user, 
               row_number() OVER(PARTITION BY user_id ORDER BY time) as porydok 
        FROM user_actions
       ) as porydok_users
     WHERE porydok = 1
     GROUP BY time_user
    ) as unique_day_users

  JOIN

    (SELECT time_courier, count(time_courier) as new_couriers 
     FROM 
       (SELECT courier_id, time::date as time_courier, 
               row_number() OVER(PARTITION BY courier_id ORDER BY time) as porydok 
        FROM courier_actions
       ) as porydok_couriers
     WHERE porydok = 1
     GROUP BY time_courier
    ) as porydok_couriers

  ON time_courier = time_user
) as kolvo;



-- Задание 2 - Прирост показателей в процентах

SELECT date,
new_users,
new_couriers,
total_users,
total_couriers,
ROUND(100 * (new_users - lag(new_users, 1) over(order by date)) / lag(new_users, 1) over(order by date)::NUMERIC, 2)  as new_users_change,
ROUND(100 * (new_couriers- lag(new_couriers, 1) over(order by date)) / lag(new_couriers, 1) over(order by date)::NUMERIC, 2) as new_couriers_change,
ROUND(100 * (total_users- lag(total_users, 1) over(order by date)) / lag(total_users, 1) over(order by date)::NUMERIC, 2) as total_users_growth, 
ROUND(100 * (total_couriers- lag(total_couriers, 1) over(order by date)) / lag(total_couriers, 1) over(order by date)::NUMERIC, 2) as total_couriers_growth
FROM 
(SELECT date, new_users, new_couriers, sum(new_users) over(order by date)::INTEGER as total_users, sum(new_couriers) over(order by date)::INTEGER  as total_couriers FROM 
  (SELECT time_courier as date, new_users, new_couriers FROM 
   (SELECT time_user, count(time_user) as new_users FROM 
    (SELECT user_id, time::date as time_user, row_number() OVER(PARTITION BY user_id ORDER BY time) as porydok FROM user_actions
     order by user_id) as porydok_users
   WHERE porydok = 1
   group by time_user
   order by time_user) as unique_day_users
 
   JOIN
 
    (SELECT time_courier, count(time_courier) as new_couriers FROM 
      (SELECT courier_id, time::date as time_courier, row_number() OVER(PARTITION BY courier_id ORDER BY time) as porydok FROM courier_actions
       order by courier_id) as porydok_couriers
     WHERE porydok = 1
     group by time_courier
     order by time_courier) as porydok_couriers
 
   on time_courier = time_user) as kolvo) as prirost
   
   
   
   
-- Задание 3 - Платящие пользователи и активные курьеры

 WITH plat as (
   SELECT order_id
   FROM user_actions
   group by order_id
   HAVING count(order_id) = 1
   order by order_id
   ),
   
   dostavka as ( 
   SELECT order_id
   FROM courier_actions
   group by order_id
   HAVING count(order_id) = 2
   order by order_id)
   

SELECT date,
paying_users,
active_couriers,
ROUND(paying_users * 100 / total_users::NUMERIC, 2) as paying_users_share,
ROUND(active_couriers * 100 / total_couriers::NUMERIC, 2) as active_couriers_share
FROM 
 (SELECT date, paying_users, sum(new_users) over(order by date)::INTEGER as total_users, paying_couriers as active_couriers,  sum(new_couriers) over(order by date)::INTEGER  as total_couriers FROM
  (SELECT  time_user as date, new_users, paying_users, new_couriers, paying_couriers FROM 
   (SELECT time_user, count(time_user) FILTER (WHERE porydok = 1) as new_users, count(DISTINCT user_id) FILTER (WHERE order_id in (SELECT * FROM plat)) as paying_users FROM 
      (SELECT order_id, user_id, time::date as time_user, row_number() OVER(PARTITION BY user_id ORDER BY time) as porydok FROM user_actions
       order by user_id) as porydok_users
     group by time_user
     order by time_user) as porydok_users
    
     JOIN
   
     (SELECT time_courier, count(time_courier) FILTER (WHERE porydok = 1) as new_couriers, count(DISTINCT courier_id) FILTER (WHERE order_id in (SELECT * FROM dostavka)) as paying_couriers FROM 
      (SELECT order_id, courier_id, time::date as time_courier, row_number() OVER(PARTITION BY courier_id ORDER BY time) as porydok FROM courier_actions
       order by courier_id) as porydok_couriers
     group by time_courier
     order by time_courier) as porydok_couriers
   
     on time_user = time_courier) as spisok) as pay_total
     
     
 
 -- Задание 4 - Повторные и единичные заказы пользователей
     
   SELECT order_id
   FROM user_actions
   group by order_id
   HAVING count(order_id) = 1
   order by order_id
   )
   
SELECT date, ROUND(edinic * 100 / paying_users::NUMERIC, 2) as single_order_users_share, ROUND(mnogo * 100 / paying_users::NUMERIC, 2) as several_orders_users_share FROM 
 (SELECT pay_users.time_user as date, paying_users, edinic, mnogo FROM 
  (SELECT time_user, count(DISTINCT user_id) FILTER (WHERE order_id in (SELECT * FROM plat)) as paying_users FROM 
       (SELECT order_id, user_id, time::date as time_user, row_number() OVER(PARTITION BY user_id ORDER BY time) as porydok FROM user_actions
        order by user_id) as porydok_users
      group by time_user
      order by time_user) as pay_users
   
    JOIN
    
  (SELECT time_user, count(user_id) FILTER(WHERE kolvo = 1) as edinic, count(user_id) FILTER(WHERE kolvo > 1) as mnogo FROM 
       (SELECT time::date as time_user, user_id, count(user_id) as kolvo FROM user_actions
        WHERE order_id in (SELECT * FROM plat)
        group by time_user, user_id) as kolvo_zakazov
    group by time_user
    order by time_user) as zakazy 

on zakazy.time_user = pay_users.time_user) as kolvo_users



 -- Задание 5 - Первые заказы и заказы новых пользователей

WITH plat as (
   SELECT order_id
   FROM user_actions
   group by order_id
   HAVING count(order_id) = 1
   order by order_id
   ),
   
   dostavka as ( 
   SELECT order_id
   FROM courier_actions
   group by order_id
   HAVING count(order_id) = 2
   order by order_id)
   

 
 SELECT date,
 orders,
 first_orders,
 new_users_orders,
 ROUND(first_orders * 100 / orders::NUMERIC, 2) as first_orders_share,
 ROUND(new_users_orders * 100 / orders::NUMERIC, 2) as new_users_orders_share
 FROM
  (SELECT time::DATE as date,
   count(order_id) as orders,
   count(order_id) FILTER(WHERE perv = 1) as first_orders,
   count(order_id) FILTER(WHERE perv = new_zakazy)  as new_users_orders
   FROM  
    (SELECT user_id, order_id, action, time, 
     row_number() over(PARTITION BY user_id order by time) as perv,
     row_number() over(PARTITION BY user_id, time::DATE order by time) as new_zakazy
     FROM user_actions
     WHERE order_id in (SELECT * FROM plat) and order_id in (SELECT * FROM dostavka)
     order by user_id) as kolvo
    group by date) as chislo
  order by date
  
  
  
 -- Задание 6 - Нагрузка на одного курьера
  
  WITH plat as (
   SELECT order_id
   FROM user_actions
   group by order_id
   HAVING count(order_id) = 1
   order by order_id
   ),
   
   dostavka as ( 
   SELECT order_id
   FROM courier_actions
   group by order_id
   HAVING count(order_id) = 2
   order by order_id)
   
   
   SELECT time_courier as date, ROUND(paying_users / active_couriers::NUMERIC, 2) as users_per_courier, orders_per_courier FROM
     (SELECT time_user, count(DISTINCT user_id) FILTER (WHERE order_id in (SELECT * FROM plat)) as paying_users FROM 
       (SELECT order_id, user_id, time::date as time_user, row_number() OVER(PARTITION BY user_id ORDER BY time) as porydok FROM user_actions
        order by user_id) as porydok_users
      group by time_user
      order by time_user) as users
      
      JOIN
     
    (SELECT time_courier, active_couriers, active_orders, ROUND(active_orders / active_couriers::NUMERIC, 2) as orders_per_courier FROM  
     (SELECT time_courier,
      count(DISTINCT courier_id) FILTER(WHERE order_id in (SELECT * FROM dostavka)) as active_couriers, 
      count(DISTINCT order_id) FILTER(WHERE action = 'accept_order' and order_id in (SELECT * FROM plat)) as active_orders FROM
      (SELECT order_id, courier_id, time::date as time_courier, action, row_number() OVER(PARTITION BY courier_id ORDER BY time) as porydok FROM courier_actions 
         order by courier_id) as porydok_couriers
      group by time_courier
      order by time_courier) as zakazy) as couriers
      
      on time_courier = time_user
      
      
      
  -- Задание 7 - Среднее время доставки
      
 WITH plat as (
   SELECT order_id
   FROM user_actions
   group by order_id
   HAVING count(order_id) = 1
   order by order_id
   ),
   
   dostavka as ( 
   SELECT order_id
   FROM courier_actions
   group by order_id
   HAVING count(order_id) = 2
   order by order_id)
   
   
  SELECT deliver_time::DATE as date, (avg(diff) / 60)::INTEGER as minutes_to_deliver FROM 
   (SELECT order_id, accept_time, deliver_time, EXTRACT(EPOCH from AGE(deliver_time, accept_time)) as diff FROM 
     (SELECT order_id, min(time) as accept_time, max(time) as deliver_time FROM 
      (SELECT * FROM courier_actions
      WHERE order_id in (SELECT * FROM dostavka) and order_id in (SELECT * FROM plat)) as kyrery
    group by order_id 
    order by order_id) as vremya) as vremya_zakaz 
   group by date
   order by date     
   
   
   
   -- Задание 8 - Доставка и Отмена заказов по часам
   
   WITH otmen as
   (
   SELECT order_id
   FROM user_actions
   group by order_id
   HAVING count(order_id) = 2
   order by order_id
   ),
   
   dostavka as ( 
   SELECT order_id
   FROM courier_actions
   group by order_id
   HAVING count(order_id) = 2
   order by order_id)
   

  SELECT
  hour,  
  successful_orders,
  canceled_orders,
  ROUND(canceled_orders / chislo::NUMERIC, 3) as cancel_rate
  FROM
   (SELECT hour,
   count(order_id) FILTER(WHERE action = 'accept_order') as chislo,
   count(order_id) FILTER(WHERE action = 'accept_order' and order_id in (SELECT * FROM dostavka)) as successful_orders,
   count(order_id) FILTER(WHERE action = 'accept_order' and order_id in (SELECT * FROM otmen)) as canceled_orders 
   FROM
    (SELECT order_id, action, time, DATE_PART('hour', time)::INTEGER as hour FROM courier_actions) as zakazy
    group by hour) as kolvo
   order by hour 
   
   