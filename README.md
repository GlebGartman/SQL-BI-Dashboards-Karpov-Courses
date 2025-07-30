<h3 align="center">Описание проекта</h3>
<p align="center">
Проект выполнен в рамках курса <strong>Karpov.Courses</strong> и посвящён анализу ключевых бизнес-метрик розничной сети с использованием SQL и визуализаций в Redash.  
Основная задача — построение аналитических отчётов по данным из базы <strong>PostgreSQL</strong> с акцентом на динамику пользователей и курьеров.  
В рамках проекта рассчитывались и визуализировались следующие показатели:  
число новых пользователей, прирост пользователей, число платящих пользователей,  
число новых курьеров, прирост курьеров, число активных курьеров и т.д..  
Для анализа использовались SQL-запросы различной сложности: LEFT/RIGHT/INNER JOIN, оконные функции, агрегаты, CTE.
</p>


<details>
<summary><strong>Задачи</strong></summary>


<summary><strong>Задание 1: Динамика пользователей и курьеров</strong></summary>

📌 Рассчитаны следующие показатели для каждого дня:
- `new_users` — число новых пользователей  
- `new_couriers` — число новых курьеров  
- `total_users` — накопительное число пользователей  
- `total_couriers` — накопительное число курьеров  
- `date` — дата события  

🔢 Все значения приведены в виде целых чисел. Результат отсортирован по дате по возрастанию

---


<summary><strong>Задание 2: Прирост показателей в процентах</strong></summary>

📌 Дополнен запрос из предыдущего задания для расчёта относительной динамики:

- `new_users_change` — прирост числа новых пользователей (%)
- `new_couriers_change` — прирост числа новых курьеров (%)
- `total_users_growth` — прирост общего числа пользователей (%)
- `total_couriers_growth` — прирост общего числа курьеров (%)

📐 Все значения округлены до двух знаков после запятой.  
📅 Результат отсортирован по дате в порядке возрастания.

---

<summary><strong>Задание 3: Платящие пользователи и активные курьеры</strong></summary>

📌 Для каждого дня были рассчитаны следующие показатели:

- `paying_users` — число платящих пользователей  
- `active_couriers` — число активных курьеров  
- `paying_users_share` — доля платящих пользователей (%)  
- `active_couriers_share` — доля активных курьеров (%)  
- `date` — дата

📐 Абсолютные значения представлены целыми числами.  
📊 Доли выражены в процентах и округлены до двух знаков после запятой.  
📅 Результат отсортирован по дате в порядке возрастания.

---

<summary><strong>Задание 4: Повторные и единичные заказы пользователей</strong></summary>

📌 Для каждого дня рассчитаны доли платящих пользователей:

- `single_order_users_share` — доля пользователей, сделавших **один заказ**  
- `several_orders_users_share` — доля пользователей, сделавших **более одного заказа**  
- `date` — дата

📊 Доли рассчитаны от общего числа платящих пользователей за день, выражены в процентах и округлены до двух знаков после запятой.  
📅 Результаты отсортированы по возрастанию даты.

---

<summary><strong>Задание 5: Первые заказы и заказы новых пользователей</strong></summary>

📌 Для каждого дня рассчитаны следующие показатели:

- `orders` — общее число заказов  
- `first_orders` — число **первых заказов** пользователей  
- `new_users_orders` — число заказов, сделанных **в день первого использования**  
- `first_orders_share` — доля первых заказов от общего числа заказов (%)  
- `new_users_orders_share` — доля заказов новых пользователей от общего числа заказов (%)  
- `date` — дата

🔢 Количественные показатели выражены целыми числами.  
📊 Доли рассчитаны в процентах, округлены до двух знаков после запятой.  
📅 Результат отсортирован по возрастанию даты.

---

<summary><strong>Задание 6: Нагрузка на одного курьера</strong></summary>

📌 Для каждого дня рассчитаны показатели нагрузки на одного активного курьера:

- `users_per_courier` — число платящих пользователей на одного активного курьера  
- `orders_per_courier` — число заказов на одного активного курьера  
- `date` — дата

📊 Показатели округлены до двух знаков после запятой.  
📅 Результаты отсортированы по дате в порядке возрастания.

---

<summary><strong>Задание 7: Среднее время доставки</strong></summary>

📌 Для каждого дня рассчитан следующий показатель:

- `minutes_to_deliver` — среднее время доставки заказов в минутах  
- `date` — дата

⏱ В расчётах учитывались только **доставленные заказы**, отменённые — исключены.  
📊 Среднее время доставки округлено до целых минут.  
📅 Результат отсортирован по дате в порядке возрастания.

---
<summary><strong>Задание 8: Доставка и Отмена заказов по часам</strong></summary>

📌 Для каждого часа суток рассчитаны следующие показатели:

- `successful_orders` — число доставленных заказов  
- `canceled_orders` — число отменённых заказов  
- `cancel_rate` — доля отменённых заказов в общем числе заказов  
- `hour` — час оформления заказа (от 0 до 23)

📊 Доля отмен рассчитана в **доле единицы** и округлена до **трёх знаков после запятой**.  
📅 Результат отсортирован по возрастанию колонки `hour`.


</details>


<details>
<summary><strong>Коды SQL и Визуализации</strong></summary>       
<summary><strong>Задание 1: Код и график — Динамика пользователей и курьеров</strong></summary>

### Код

```sql
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
```

### Динамика новых пользователей и курьеров

![График новых пользователей и курьеров](https://drive.google.com/uc?export=view&id=1utO-05YZpRS3nRqrh6x_8n9m1BiIJjgs)

### Динамика общего числа пользователей

![График общего числа пользователей](https://drive.google.com/uc?export=view&id=1e-nVF563jSuhsUVFSUA3gTwyMko3EB8y)

---

<summary><strong>Задание 2: Код и график — Прирост показателей в процентах</strong></summary>

### Код

```sql
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
```

### Динамика новых пользователей и курьеров

![Динамика прироста числа новых пользователей и курьеров](https://drive.google.com/uc?export=view&id=1CiWQGpS8T5Z0BNDC18igPf8G5adOpr7q)

### Динамика общего числа пользователей

![Динамика прироста общего числа пользователей и курьеров](https://drive.google.com/uc?export=view&id=1icXQY02osg4VnqJoHWMnL04OBT_scHhn)

---


<summary><strong>Задание 3: Код и график —  Платящие пользователи и активные курьеры</strong></summary>

### Код

```sql
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
```

### Динамика активности платящих пользователей и курьеров

![График: платящие пользователи и активные курьеры](https://drive.google.com/uc?export=view&id=1eIjAjc-Q1jPW0GJRCErM5_9g493P94Om)

### Доля платящих пользователей и активных курьеров

![График: доля платящих пользователей и активных курьеров](https://drive.google.com/uc?export=view&id=1BzlEcj1iwV6rgeaHPCrMADZDy1UkptpW)


---


<summary><strong>Задание 4: Код и график —  Повторные и единичные заказы пользователей</strong></summary>

### Код

```sql
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

```

### Доли пользователей с одним и несколькими заказами

![График: доли пользователей с одним и несколькими заказами](https://drive.google.com/uc?export=view&id=1JNS3PEi35YFaQeru784HHRwtrv6HRpvT)


---

<summary><strong>Задание 5: Код и график - Первые заказы и заказы новых пользователей</strong></summary>

### Код

```sql
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


```

### Динамика общего числа заказов, первых заказов и заказов новых пользователей

![График: общее число заказов, первые заказы и заказы новых пользователей](https://drive.google.com/uc?export=view&id=1wgOsmV1aESJPmckZXNmBGJSj4wJSLXli)

### Доля первых заказов и заказов новых пользователей в общем числе заказов

![График: доля первых заказов и заказов новых пользователей](https://drive.google.com/uc?export=view&id=156PJkrx9Tb4US5EjVopOTwiuGlorO2vI)


---

<summary><strong>Задание 6: Код и график - Нагрузка на одного курьера</strong></summary>

### Код

```sql
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
      
```

### Динамика числа пользователей и заказов на одного курьера

![График: пользователи и заказы на одного курьера](https://drive.google.com/uc?export=view&id=1Pom84jhHidr3iB1dfb6aTADgSg6ymWQ8)

---


<summary><strong>Задание 7: Код и график - Среднее время доставки</strong></summary>

```sql
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

```

### Динамика среднего времени доставки заказов

![График: среднее время доставки](https://drive.google.com/uc?export=view&id=1TlQhF3_v7Y1XD-A1Y5vugl0u2GPsJBrE)

---

<summary><strong>Задание 8: Код и график - Доставка и Отмена заказов по часам</strong></summary>

```sql
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
```

### Динамика cancel rate и числа успешных/отменённых заказов по часам

![График: cancel rate и количество заказов](https://drive.google.com/uc?export=view&id=1z2zTZzIKf-tpcVcs0r8zddHh7C16wWJF)



</details>

<details> 

<summary><strong>Выводы</strong></summary>

📌 На основе рассчитанных показателей и визуализированных графиков был построен итоговый дашборд.

🔗 [Открыть дашборд в Redash](https://redash.public.karpov.courses/public/dashboards/rUhrM7LM2eXf6rI0F6y5aENIzXNnEGr8zJImLWk8?org_slug=default)

</details>
