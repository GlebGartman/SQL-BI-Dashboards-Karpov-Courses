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
<summary><strong>Задания</strong></summary>


<summary><strong>Задание 1: Динамика пользователей и курьеров</strong></summary>

📌 Рассчитаны следующие показатели для каждого дня:
- `new_users` — число новых пользователей  
- `new_couriers` — число новых курьеров  
- `total_users` — накопительное число пользователей  
- `total_couriers` — накопительное число курьеров  
- `date` — дата события  

🔢 Все значения приведены в виде целых чисел. Результат отсортирован по дате по возрастанию

---
</details>


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




</details>

<details> 

<summary><strong>Выводы</strong></summary>
