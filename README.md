```markdown
# SQL Task Solutions with Approach Analysis

В этом репозитории собраны решения аналитических SQL-задач, выполненные в едином стиле. Для каждой задачи представлены один или несколько вариантов запросов с подробным разбором плюсов и минусов каждого подхода.

Все запросы написаны в едином стиле:
- Ключевые слова заглавными буквами
- Отступы для улучшения читаемости
- Имена CTE и алиасов в `snake_case`
- Явные указания `JOIN` и условий

---

## Содержание
1. [Топ-товары в «премиальных» категориях](#топ-товары-в-премиальных-категориях)
2. [Быстрая активация клиентов до марта](#быстрая-активация-клиентов-до-марта)
3. [Герой-товар по отделу с учётом возвратов](#герой-товар-по-отделу-с-учётом-возвратов)
4. [Активнее среднего в регионе «Сибирь»](#активнее-среднего-в-регионе-сибирь)
5. [Сегментация клиентов по активности и выручке](#сегментация-клиентов-по-активности-и-выручке)
6. [Потенциальный дефицит на складе](#потенциальный-дефицит-на-складе)
7. [Ежедневный CTR с учётом повторных кликов и регистраций](#ежедневный-ctr-с-учётом-повторных-кликов-и-регистраций)
8. [Раздражённые клиенты и tricky-обращения](#раздражённые-клиенты-и-tricky-обращения)
9. [Заказы с нарушением сроков доставки](#заказы-с-нарушением-сроков-доставки)
10. [Анализ успеваемости по экзаменам](#анализ-успеваемости-по-экзаменам)
11. [Рейтинг клиентов по сумме заказов за последние 90 дней](#рейтинг-клиентов-по-сумме-заказов-за-последние-90-дней)
12. [Клиенты, купившие A и B в разные даты с разрывом ≥ 7 дней](#клиенты-купившие-a-и-b-в-разные-даты-с-разрывом--7-дней)
13. [Рейтинг задач по сложности и интересности для Авито](#рейтинг-задач-по-сложности-и-интересности-для-авито)

---

## Топ-товары в «премиальных» категориях

**Условие**  
Маркетинговая команда хочет определить «премиальные» категории — те, в которых средняя выручка на товар выше общей средней по всем категориям. Для каждой такой категории нужно выбрать топ‑1 товар по выручке (если несколько товаров имеют одинаковую максимальную выручку — вывести все). Рассматриваются только завершённые заказы за период с 2025‑01‑01 по 2025‑06‑30.

**Таблицы**: `categories`, `products`, `order_items`.

### Решения

| # | Подход | Плюсы | Минусы |
|---|--------|-------|--------|
| 1 | Многоступенчатые CTE | Лёгкая читаемость, каждый шаг можно проверить отдельно. Подходит для обучения и отладки. | Избыточное количество CTE, возможны повторные сканирования. В текущей версии неверный ORDER BY (отсутствует category_name). |
| 2 | CTE + DENSE_RANK | Компактнее первого, корректно обрабатывает совпадения выручки благодаря DENSE_RANK. Хороший баланс между читаемостью и производительностью. | Подзапрос для общего среднего может вычисляться отдельно, но в целом не критично. |
| 3 | Оконные функции над агрегатами | Максимально лаконично: все расчёты в одном CTE. Элегантное использование оконных функций после GROUP BY. | Может быть сложно для понимания новичками; в некоторых СУБД оконные функции после агрегации могут иметь особенности производительности. |

#### Решение 1.1 (многоступенчатые CTE)
```sql
WITH
    revenue_by_product AS
    (
    SELECT
        p.category_id,
        p.product_id,
        SUM(oi.price * oi.quantity) AS total_sales
    FROM
        order_items oi
    JOIN
        products p
    ON    
        oi.product_id = p.product_id
        AND oi.status = 'Completed'
        AND oi.price IS NOT NULL
        AND oi.quantity IS NOT NULL
        AND oi.order_date BETWEEN '2025-01-01' AND '2025-06-30'
    GROUP BY
        p.category_id,
        p.product_id
    ),
    avg_product_revenue_by_cat AS 
    (
    SELECT
        category_id,
        AVG(total_sales) AS avg_rev_category
    FROM
        revenue_by_product
    GROUP BY
        category_id
    ),
    cats_where_avg_rev_more_than_over AS
    (
    SELECT
        category_id
    FROM
        avg_product_revenue_by_cat
    WHERE
        avg_rev_category > (
                            SELECT 
                                SUM(total_sales) / 
                                NULLIF(COUNT(DISTINCT product_id), 0) 
                            FROM revenue_by_product
                           )
    ),
    top_rev_by_cat AS
    (
    SELECT
        category_id,
        MAX(total_sales) AS top_product_rev
    FROM
        revenue_by_product
    WHERE
        category_id IN (SELECT category_id FROM cats_where_avg_rev_more_than_over)
    GROUP BY
        category_id
    )
SELECT
    c.name AS category_name,
    p.name AS product_name,
    rbp.total_sales
FROM
    revenue_by_product rbp
JOIN
    cats_where_avg_rev_more_than_over vipc
ON
    rbp.category_id = vipc.category_id
JOIN
    top_rev_by_cat trbc
ON
    rbp.category_id = trbc.category_id
    AND rbp.total_sales = trbc.top_product_rev
JOIN
    products p
ON
    rbp.product_id = p.product_id
JOIN
    categories c
ON
    trbc.category_id = c.category_id
ORDER BY
    total_sales,
    product_name;   -- Ошибка: забыт category_name
```

#### Решение 1.2 (CTE + DENSE_RANK)
```sql
WITH
    rev_by_product AS
    (
    SELECT
        p.category_id,
        p.product_id,
        SUM(oi.price * oi.quantity) AS total_sales
    FROM
        order_items oi
    JOIN
        products p
    ON
        oi.product_id = p.product_id
    WHERE
        oi.order_date BETWEEN '2025-01-01' AND '2025-06-30'
        AND oi.status = 'Completed'
        AND oi.price IS NOT NULL
        AND oi.quantity IS NOT NULL
    GROUP BY
        p.category_id,
        p.product_id
    ),
    top_cats AS
    (
    SELECT
        category_id
    FROM
        rev_by_product
    GROUP BY
        category_id
    HAVING
        AVG(total_sales) > (SELECT AVG(total_sales) FROM rev_by_product)
    ),
    ranked_prods_in_top_cats AS
    (
    SELECT
        rbp.category_id,
        rbp.product_id,
        rbp.total_sales,
        DENSE_RANK() OVER(PARTITION BY rbp.category_id ORDER BY rbp.total_sales DESC) AS prod_rnk
    FROM
        rev_by_product rbp
    JOIN
        top_cats tc
    ON
        rbp.category_id = tc.category_id
    )
SELECT
     c.name AS category_name,
     p.name AS product_name,
     rpitc.total_sales
FROM
    ranked_prods_in_top_cats rpitc
JOIN
    products p
ON
    rpitc.product_id = p.product_id
JOIN
    categories c
ON
    p.category_id = c.category_id
WHERE
    prod_rnk = 1
ORDER BY
    category_name,
    total_sales,
    product_name;
```

#### Решение 1.3 (оконные функции над агрегатами)
```sql
WITH 
    rev_by_prod AS 
    (
    SELECT
        p.category_id,
        p.product_id,
        SUM(oi.price * oi.quantity) AS product_rev,
        AVG(SUM(oi.price * oi.quantity)) OVER(PARTITION BY p.category_id) AS avg_cat_rev,
        AVG(SUM(oi.price * oi.quantity)) OVER() AS overall_avg_rev
    FROM 
        order_items oi
    JOIN 
        products p 
    ON 
        oi.product_id = p.product_id
    WHERE 
        oi.order_date BETWEEN '2025-01-01' AND '2025-06-30' 
        AND oi.status = 'Completed'
        AND oi.price IS NOT NULL 
        AND oi.quantity IS NOT NULL
    GROUP BY 
        p.category_id, 
        p.product_id
    ),
    top_cats AS 
    (
    SELECT
        category_id,
        product_id,
        product_rev,
        DENSE_RANK() OVER(PARTITION BY category_id ORDER BY product_rev DESC) AS dr
    FROM 
        rev_by_prod
    WHERE 
        avg_cat_rev > overall_avg_rev
    )
SELECT
    c.name AS category_name,
    p.name AS product_name,
    tc.product_rev AS total_sales
FROM 
    top_cats tc
JOIN 
    products p 
ON 
    tc.product_id = p.product_id
JOIN 
    categories c 
ON 
    tc.category_id = c.category_id
WHERE 
    tc.dr = 1
ORDER BY 
    category_name, 
    total_sales, 
    product_name;
```

---

## Быстрая активация клиентов до марта

**Условие**  
Нужно найти клиентов, которые совершили минимум 3 заказа, причём первый заказ был строго раньше 2025‑03‑01, а третий заказ уложился в 30 календарных дней после первого. Вывести `customer_id` и дату третьего заказа.

**Таблица**: `orders`.

### Решения

| # | Подход | Плюсы | Минусы |
|---|--------|-------|--------|
| 1 | Классические CTE с ROW_NUMBER | Понятная пошаговая логика, легко модифицировать. | Немного избыточно (first_order_date дублируется). |
| 2 | Оконные функции в одном CTE | Очень компактно, все нужные поля вычисляются на лету. | При больших объёмах данных оконные функции могут быть тяжелее, но для учебных задач оптимально. |
| 3 | Отдельное CTE для третьих заказов + JOIN | Альтернативный способ явно выделить первый и третий заказы. | Два обращения к одному CTE, что может снизить производительность. |
| 4 | JOIN filtered_clients и numbered_orders | Похоже на решение 1, но условие проверяется в финальном WHERE. | Практически эквивалентно решению 1. |
| 5 | CTE с включением first_order_date в нумерацию | Самый сбалансированный: все нужные поля в одном CTE, хорошая читаемость. | Недостатков не выделено. |

#### Решение 2.1 (классические CTE с ROW_NUMBER)
```sql
WITH
    filtered_clients AS
    (
    SELECT
        customer_id,
        MIN(order_date) AS first_order_date
    FROM
        orders
    WHERE
        order_date < '2025-04-01'
    GROUP BY
        customer_id
    HAVING
        MIN(order_date) < '2025-03-01'
        AND COUNT(*) >= 3
    ),
    numbered_orders AS
    (
    SELECT
        fc.customer_id,
        o.order_date,
        fc.first_order_date,
        ROW_NUMBER() OVER(PARTITION BY fc.customer_id ORDER BY o.order_date) AS order_num
    FROM
        filtered_clients fc
    JOIN
        orders o
    ON
        fc.customer_id = o.customer_id
    )
SELECT
    customer_id,
    order_date AS third_order_date
FROM
    numbered_orders numo
WHERE
    order_num = 3
    AND order_date <= first_order_date + INTERVAL 30 DAY  
ORDER BY
    customer_id,
    third_order_date;
```

#### Решение 2.2 (оконные функции в одном CTE)
```sql
WITH 
    customer_stats AS 
    (
    SELECT
        customer_id,
        order_date,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_date) AS rn,
        MIN(order_date) OVER (PARTITION BY customer_id) AS first_order,
        COUNT(*) OVER (PARTITION BY customer_id) AS total_orders
    FROM 
        orders
)
SELECT
    customer_id,
    order_date AS third_order_date
FROM 
    customer_stats
WHERE
    rn = 3
    AND first_order < '2025-03-01'
    AND total_orders >= 3
    AND order_date <= first_order + INTERVAL 30 DAY
ORDER BY 
    customer_id;
```

#### Решение 2.3 (отдельное CTE для третьих заказов + JOIN)
```sql
WITH
    filtered_clients AS
    (
    SELECT
        customer_id
    FROM
        orders
    WHERE
        order_date < '2025-04-01'
    GROUP BY
        customer_id
    HAVING
        MIN(order_date) < '2025-03-01'
        AND COUNT(*) >= 3
    ),
    numbered_orders AS
    (
    SELECT
        fc.customer_id,
        o.order_date,
        ROW_NUMBER() OVER(PARTITION BY fc.customer_id ORDER BY o.order_date) AS order_num
    FROM
        filtered_clients fc
    JOIN
        orders o
    ON
        fc.customer_id = o.customer_id
    ),
    third_orders AS
    (
    SELECT
        customer_id,
        order_date AS third_order_date
    FROM
        numbered_orders 
    WHERE
        order_num = 3
    )
SELECT
    tor.customer_id,
    tor.third_order_date
FROM
    third_orders tor
JOIN
    numbered_orders nor
ON
    tor.customer_id = nor.customer_id
    AND nor.order_num = 1
WHERE
    tor.third_order_date BETWEEN nor.order_date AND nor.order_date + INTERVAL 30 DAY
ORDER BY
    customer_id,
    third_order_date;
```

#### Решение 2.4 (JOIN filtered_clients и numbered_orders)
```sql
WITH
    filtered_clients AS
    (
    SELECT
        customer_id,
        MIN(order_date) AS first_order_date
    FROM
        orders
    WHERE
        order_date < '2025-04-01'
    GROUP BY
        customer_id
    HAVING
        MIN(order_date) < '2025-03-01'
        AND COUNT(*) >= 3
    ),
    numbered_orders AS
    (
    SELECT
        fc.customer_id,
        o.order_date,
        ROW_NUMBER() OVER(PARTITION BY fc.customer_id ORDER BY o.order_date) AS order_num
    FROM
        filtered_clients fc
    JOIN
        orders o
    ON
        fc.customer_id = o.customer_id
    )
SELECT
    numo.customer_id,
    numo.order_date AS third_order_date
FROM
    filtered_clients fc
JOIN
    numbered_orders numo
ON
    fc.customer_id = numo.customer_id
WHERE
    numo.order_num = 3
    AND numo.order_date <= fc.first_order_date + INTERVAL 30 DAY  
ORDER BY
    customer_id,
    third_order_date;
```

#### Решение 2.5 (CTE с включением first_order_date в нумерацию)
```sql
WITH
    filtered_clients AS
    (
    SELECT
        customer_id,
        MIN(order_date) AS first_order_date
    FROM
        orders
    WHERE
        order_date < '2025-04-01'
    GROUP BY
        customer_id
    HAVING
        MIN(order_date) < '2025-03-01'
        AND COUNT(*) >= 3
    ),
    numbered_orders AS
    (
    SELECT
        fc.customer_id,
        o.order_date,
        fc.first_order_date,
        ROW_NUMBER() OVER(PARTITION BY fc.customer_id ORDER BY o.order_date) AS order_num
    FROM
        filtered_clients fc
    JOIN
        orders o
    ON
        fc.customer_id = o.customer_id
    )
SELECT
    customer_id,
    order_date AS third_order_date
FROM
    numbered_orders numo
WHERE
    order_num = 3
    AND order_date <= first_order_date + INTERVAL 30 DAY  
ORDER BY
    customer_id,
    third_order_date;
```

---

## Герой-товар по отделу с учётом возвратов

**Условие**  
Для каждого отдела за II квартал 2025 года (01.04–30.06) определить товар с максимальной **net-выручкой** (выручка за вычетом возвратов, попавших в тот же квартал). Учитываются только завершённые продажи, игнорируются NULL-поля. При равенстве net-выручки выбирается товар с более ранней первой продажей, а если и она совпадает — с меньшим `item_id`.

**Таблицы**: `departments`, `items`, `sales`, `refunds`.

### Решения

| # | Подход | Плюсы | Минусы |
|---|--------|-------|--------|
| 1 | Раздельные CTE для продаж и возвратов | Чёткая структура, легко контролировать фильтры. Подходит для сложной логики. | Потенциальная ошибка при множественных возвратах на один sale_id: в CTE `filtered_refs` не выполняется агрегация, что может привести к дублированию строк в LEFT JOIN и неверному расчёту выручки. |
| 2 | Один запрос с LEFT JOIN и оконной функцией | Компактность, все данные собираются в одном месте. Правильно используется MIN для tie-break. | Та же проблема с множественными возвратами, если не гарантирована уникальность `sale_id` в таблице `refunds`. При наличии нескольких возвратов на одну продажу сумма вычтется несколько раз. |

#### Решение 3.1 (раздельные CTE для продаж и возвратов)
```sql
WITH
    filtered_sales AS
    (
    SELECT
        sale_id,
        item_id,
        quantity,
        price,
        sale_date
    FROM
        sales 
    WHERE
        sale_date BETWEEN '2025-04-01' AND '2025-06-30'
        AND status = 'Completed'
        AND price IS NOT NULL
        AND quantity IS NOT NULL
    ),
    filtered_refs AS
    (
    SELECT
        sale_id,
        item_id,
        quantity
    FROM
        refunds 
    WHERE
        refund_date BETWEEN '2025-04-01' AND '2025-06-30'
        AND quantity IS NOT NULL
    ),
    net_rev AS
    (
    SELECT
        fs.item_id,
        SUM(fs.price * fs.quantity - COALESCE(fs.price * fr.quantity, 0)) AS total_revenue,
        MIN(fs.sale_date) AS first_sale_date
    FROM
        filtered_sales fs
    LEFT JOIN
        filtered_refs fr
    ON
        fs.sale_id = fr.sale_id
        AND fs.item_id = fr.item_id
    GROUP BY
        fs.item_id
    ),
    ranked_items_by_dept AS
    (
    SELECT
        d.dept_name,
        i.name AS item_name,
        nr.total_revenue,
        ROW_NUMBER() OVER w AS item_rn
    FROM
        net_rev nr
    JOIN
        items i
    ON
        nr.item_id = i.item_id
    JOIN
        departments d
    ON
        i.dept_id = d.dept_id
    WINDOW
        w AS
        (
        PARTITION BY d.dept_id
        ORDER BY nr.total_revenue DESC, nr.first_sale_date, nr.item_id
        )
    )
SELECT
    dept_name,
    item_name,
    ROUND(total_revenue, 2) AS total_revenue
FROM
    ranked_items_by_dept
WHERE
    item_rn = 1
ORDER BY
    dept_name;
```

#### Решение 3.2 (один запрос с LEFT JOIN и оконной функцией)
```sql
WITH
    ranked_items_by_dept AS
    (
    SELECT
        d.dept_name,
        i.name AS item_name,
        SUM(fs.price * (fs.quantity - COALESCE(fr.quantity, 0))) AS total_revenue,
        ROW_NUMBER() OVER w AS item_rn
    FROM
        sales fs
    LEFT JOIN
        refunds fr
    ON
        fs.sale_id = fr.sale_id
        AND fs.item_id = fr.item_id
        AND fr.refund_date BETWEEN '2025-04-01' AND '2025-06-30'
        AND fr.quantity IS NOT NULL
    JOIN
        items i
    ON
        fs.item_id = i.item_id
    JOIN
        departments d
    ON
        i.dept_id = d.dept_id
    WHERE
        fs.sale_date BETWEEN '2025-04-01' AND '2025-06-30'
        AND fs.status = 'Completed'
        AND fs.price IS NOT NULL
        AND fs.quantity IS NOT NULL
    GROUP BY
        fs.item_id,
        i.name,
        d.dept_id,
        d.dept_name
    WINDOW
        w AS
        (
        PARTITION BY d.dept_id
        ORDER BY 
            SUM(fs.price * (fs.quantity - COALESCE(fr.quantity, 0))) DESC, 
            MIN(fs.sale_date), 
            fs.item_id
        )
    )
SELECT
    dept_name,
    item_name,
    ROUND(total_revenue, 2) AS total_revenue
FROM
    ranked_items_by_dept
WHERE
    item_rn = 1
ORDER BY
    dept_name;
```

> **Важное замечание**: Если в таблице `refunds` может быть несколько записей на один `sale_id`, приведённые запросы дадут неверный результат (вычтут возврат несколько раз). Корректная реализация должна предварительно агрегировать возвраты (например, через GROUP BY sale_id).

---

## Активнее среднего в регионе «Сибирь»

**Условие**  
Найти пользователей из региона 'Сибирь', которые в июне 2024 оставили строго больше комментариев, чем среднее число комментариев на пользователя в этом регионе за тот же период. Для каждого такого пользователя рассчитать долю его комментариев в общем числе комментариев региона (в процентах, округлить до 1 знака). Среднее считается по всем пользователям региона, включая тех, кто не оставлял комментарии.

**Таблицы**: `users`, `comments`.

### Решения

| # | Подход | Плюсы | Минусы |
|---|--------|-------|--------|
| 1 | Многоступенчатые CTE с оконными функциями | Чёткое разделение этапов: сначала пользователи региона, затем счётчики. Использование NULLIF защищает от деления на ноль. | Довольно громоздко; избыточное CROSS JOIN с числом пользователей. |
| 2 | Компактный запрос с двумя CTE | Лаконичнее, логика понятна. Быстрое получение общей статистики региона. | При отсутствии комментариев регион не попадёт в выборку, но это соответствует условию. |

#### Решение 4.1 (многоступенчатые CTE)
```sql
WITH
    sibir_users AS 
    (
    SELECT
        user_id,
        name
    FROM
        users
    WHERE
        region = 'Сибирь'
    ),
    sibir_users_cnt AS
    (
    SELECT 
        NULLIF(COUNT(*), 0) AS all_users_cnt
    FROM
        sibir_users
    ),
    sibir_users_comments_cnt AS
    (
    SELECT
        su.name,
        COUNT(*) AS comment_count,
        NULLIF(SUM(COUNT(*)) OVER(), 0) AS all_comment_cnt,
        SUM(COUNT(*)) OVER() / suc.all_users_cnt AS avg_comments_per_user
    FROM
        sibir_users su
    JOIN
        comments c
    ON
        su.user_id = c.user_id
    CROSS JOIN
        sibir_users_cnt suc
    WHERE
        c.comment_date BETWEEN '2024-06-01' AND '2024-06-30'
    GROUP BY
        su.user_id,
        su.name,
        suc.all_users_cnt
    )
SELECT
    name,
    comment_count,
    ROUND(
        comment_count * 100.0 / 
        all_comment_cnt, 1
    ) AS contribution_percent
FROM
    sibir_users_comments_cnt
WHERE
    comment_count > avg_comments_per_user
ORDER BY
    contribution_percent DESC,
    name;
```

#### Решение 4.2 (компактный запрос)
```sql
WITH
    comments_cnt AS
    (
    SELECT
        u.name,
        COUNT(*) AS comment_count
    FROM
        users u
    JOIN
        comments c
    ON
        u.user_id = c.user_id
        AND u.region = 'Сибирь'
    WHERE
        c.comment_date BETWEEN '2024-06-01' AND '2024-06-30'
    GROUP BY
        u.user_id,
        u.name
    ),
    region_stats AS
    (
    SELECT
        SUM(comment_count) AS reg_total,
        AVG(comment_count) AS reg_avg
    FROM
        comments_cnt
    )
SELECT
    name,
    comment_count,
    ROUND(comment_count / NULLIF(rs.reg_total, 0) * 100.0, 1) AS contribution_percent
FROM
    comments_cnt cc1
CROSS JOIN
    region_stats rs
WHERE
    cc1.comment_count > rs.reg_avg
ORDER BY
    contribution_percent DESC,
    name;
```

---

## Сегментация клиентов по активности и выручке

**Условие**  
Сформировать витрину сегментов клиентов на основе истории заказов. Для каждого клиента рассчитать метрики и присвоить сегмент по приоритету:
- `Inactive` — нет заказов
- `VIP` — суммарная выручка > 1000
- `New` — последний заказ не позднее 30 дней до даты расчёта (2025-08-01)
- `Regular` — остальные

Также добавить флаги `is_active` (последний заказ менее 90 дней назад) и `is_outlier` (средний чек > 500).

**Таблицы**: `customers`, `orders`.

### Решение

| # | Подход | Плюсы | Минусы |
|---|--------|-------|--------|
| 1 | Одно CTE с LEFT JOIN и CASE | Все метрики считаются в одном месте, просто и наглядно. Корректно обрабатываются клиенты без заказов благодаря LEFT JOIN. | В условии VIP указано порог > 1000, а в коде > 10000 — возможна опечатка, но структура верна. |

```sql
WITH
    customers_stats AS
    (
    SELECT
        c.customer_id,
        c.customer_name,
        COUNT(o.order_id) AS total_orders,
        ROUND(COALESCE(SUM(o.amount), 0), 2) AS total_spent,    
        ROUND(COALESCE(AVG(o.amount), 0), 2) AS avg_order, 
        MAX(o.order_date) AS last_order_date
    FROM
        customers c
    LEFT JOIN
        orders o
    ON
        c.customer_id = o.customer_id
    GROUP BY
        c.customer_id,
        c.customer_name
    )
SELECT
    customer_id,
    customer_name,
    total_orders,
    total_spent,
    avg_order,
    last_order_date,
    CASE
        WHEN total_orders = 0 THEN 'Inactive'
        WHEN total_spent > 10000 THEN 'VIP'
        WHEN last_order_date >= '2025-08-01' - INTERVAL 30 DAY THEN 'New'
        ELSE 'Regular'
    END AS segment, 
    CASE 
        WHEN last_order_date >= '2025-08-01' - INTERVAL 90 DAY THEN 1 
        ELSE 0 
    END AS is_active,
    CASE
        WHEN avg_order > 500 THEN 1
        ELSE 0
    END AS is_outlier
FROM
    customers_stats
ORDER BY
    customer_id;
```

---

## Потенциальный дефицит на складе

**Условие**  
Выявить позиции, где товар ещё есть на складе (stock > 0), но существует риск дефицита: либо средняя недельная продажа после 2025-03-01 >= текущего остатка, либо продаж не было (avg_weekly_sales = 0). Средняя считается по неделям, где были продажи, округляется до 2 знаков.

**Таблицы**: `products`, `warehouses`, `inventory`, `orders`, `order_items`.

### Решения

| # | Подход | Плюсы | Минусы |
|---|--------|-------|--------|
| 1 | Два CTE (sales, avg_sales) | Логичное разделение: сначала недельные продажи, потом средние. Хорошо читается. | Может быть чуть длиннее, но это не недостаток. |
| 2 | Оконные функции в одном CTE | Более компактно, все расчёты в одном месте. Умелое использование оконной функции AVG для получения средней по партиции. | Из-за LEFT JOIN и агрегации может потребоваться дополнительный GROUP BY, что в решении обойдено через DISTINCT в финальном SELECT, но это не очень изящно. |

#### Решение 6.1 (два CTE)
```sql
WITH
    sales AS
    (
    SELECT
        oi.product_id,
        o.warehouse_id,
        WEEK(o.order_date, 1) AS wk,
        SUM(oi.quantity) AS weekly_sales
    FROM
        orders o
    JOIN
        order_items oi
    ON
        o.order_id = oi.order_id
    WHERE
        o.order_date > '2025-03-01'
    GROUP BY
        oi.product_id,
        o.warehouse_id,
        WEEK(o.order_date, 1)
    ),
    avg_sales AS
    (
    SELECT
        product_id,
        warehouse_id,
        ROUND(AVG(weekly_sales), 2) AS avg_weekly_sales
    FROM
        sales 
    GROUP BY
        product_id,
        warehouse_id
    )
SELECT
    p.name AS product_name,
    w.name AS warehouse_name,
    i.stock,
    COALESCE(asa.avg_weekly_sales, 0) AS avg_weekly_sales
FROM
    inventory i
JOIN
    products p
ON
    i.product_id = p.product_id
JOIN
    warehouses w
ON
    i.warehouse_id = w.warehouse_id
LEFT JOIN
    avg_sales asa
ON
    i.product_id = asa.product_id
    AND i.warehouse_id = asa.warehouse_id
WHERE
    i.stock > 0 
    AND (asa.avg_weekly_sales >= i.stock OR asa.avg_weekly_sales IS NULL)
ORDER BY
    product_name,
    warehouse_name;
```

#### Решение 6.2 (оконные функции)
```sql
WITH
    weekly_sales_after_2025_03 AS
    (
    SELECT
        oi.product_id,
        o.warehouse_id,
        WEEK(o.order_date, 1) AS wk,
        SUM(oi.quantity) AS weekly_sales,
        ROUND(AVG(SUM(oi.quantity)) OVER w, 2) AS avg_weekly_sales
    FROM
        orders o
    JOIN
        order_items oi
    ON
        o.order_id = oi.order_id
    WHERE
        o.order_date > '2025-03-01'
    GROUP BY
        oi.product_id,
        o.warehouse_id,
        WEEK(o.order_date, 1)
    WINDOW
        w AS (PARTITION BY oi.product_id, o.warehouse_id)
    ),
    sales AS
    (
    SELECT
        i.product_id,    
        i.warehouse_id,
        i.stock,
        COALESCE(ws.avg_weekly_sales, 0) AS avg_weekly_sales
    FROM
        inventory i
    LEFT JOIN
        weekly_sales_after_2025_03 ws
    ON
        i.product_id = ws.product_id
        AND i.warehouse_id = ws.warehouse_id
    WHERE
        i.stock > 0
        AND (ws.avg_weekly_sales >= i.stock OR ws.avg_weekly_sales IS NULL)  
    GROUP BY
        i.product_id,    
        i.warehouse_id,
        i.stock,
        COALESCE(ws.avg_weekly_sales, 0)
    )
SELECT
    p.name AS product_name,
    w.name AS warehouse_name,
    s.stock,
    s.avg_weekly_sales
FROM
    sales s
JOIN
    products p
ON
    s.product_id = p.product_id
JOIN
    warehouses w
ON
    s.warehouse_id = w.warehouse_id
ORDER BY
    product_name,
    warehouse_name;
```

---

## Ежедневный CTR с учётом повторных кликов и регистраций

**Условие**  
Рассчитать ежедневный CTR по каналам: отношение уникальных регистраций (в течение 7 дней после клика по тому же объявлению) к уникальным кликам (уникальность по связке пользователь + дата + объявление). Регистрация засчитывается только одна на пользователя и объявление.

**Таблицы**: `adclicks`, `registrations`.

### Решение

| # | Подход | Плюсы | Минусы |
|---|--------|-------|--------|
| 1 | Один запрос с LEFT JOIN и COUNT(DISTINCT) | Лаконично, правильно учитывает уникальность кликов и регистраций. Условие на регистрацию в течение 7 дней вынесено в JOIN. | Может быть неочевидно, что COUNT(DISTINCT r.ad_id, r.user_id) работает корректно — зависит от диалекта SQL; в некоторых СУБД требуется конкатенация или явное приведение. |

```sql
SELECT
    ac.click_date,
    ac.channel,
    COUNT(DISTINCT ac.ad_id, ac.user_id) AS total_clicks,
    COUNT(DISTINCT r.ad_id, r.user_id) AS total_regs,    
    ROUND(COUNT(DISTINCT r.ad_id, r.user_id) * 100.0 / 
    NULLIF(COUNT(DISTINCT ac.ad_id, ac.user_id), 0), 2) AS ctr 
FROM
    adclicks ac
LEFT JOIN
    registrations r
ON
    ac.user_id = r.user_id
    AND ac.ad_id = r.ad_id
    AND r.registration_date BETWEEN ac.click_date AND ac.click_date + INTERVAL 7 DAY
GROUP BY
    ac.click_date,
    ac.channel
ORDER BY
    click_date,
    channel;
```

---

## Раздражённые клиенты и tricky-обращения

**Условие**  
Найти клиентов из сегмента 'retail', у которых не менее 3 обращений с датой и хотя бы 2 коротких разрыва (повторное обращение менее чем через 10 дней, включая 0). Вывести имя, общее число обращений и количество коротких разрывов.

**Таблицы**: `customers`, `support_requests`.

### Решение

| # | Подход | Плюсы | Минусы |
|---|--------|-------|--------|
| 1 | CTE с LAG + подзапрос EXISTS | Чётко выделяются обращения нужных клиентов. Использование LAG для сравнения дат. Условие на сегмент проверяется через EXISTS в CTE. | EXISTS может быть заменён на JOIN, но в данном случае он корректен. |

```sql
WITH 
    requests AS
    (
    SELECT
        customer_id,
        request_date AS cur_date,
        LAG(request_date) OVER(PARTITION BY customer_id ORDER BY request_date) AS prev_date
    FROM
        support_requests sr
    WHERE
        request_date IS NOT NULL
        AND EXISTS (
            SELECT 1
            FROM customers c
            WHERE sr.customer_id = c.customer_id
            AND c.segment = 'retail'
        )
    )
SELECT
    c.name,
    COUNT(*) AS total_requests,
    SUM(CASE WHEN cur_date BETWEEN prev_date AND prev_date + INTERVAL 9 DAY THEN 1 
        ELSE 0 END) AS short_gaps
FROM
    requests r
JOIN 
    customers c
ON
    r.customer_id = c.customer_id
GROUP BY
    c.customer_id,
    c.name
HAVING
    COUNT(*) >= 3
    AND SUM(CASE WHEN cur_date BETWEEN prev_date AND prev_date + INTERVAL 9 DAY 
            THEN 1 ELSE 0 END) >= 2
ORDER BY
    name;
```

---

## Заказы с нарушением сроков доставки

**Условие**  
Для каждого заказа с непустой датой доставки посчитать срок доставки (delivery_days). Для каждого дня оформления вычислить средний срок доставки. Оставить только те заказы, где фактический срок больше среднего по дню. Добавить разницу (delay_days) и флаг нарушения SLA (>5 дней).

**Таблицы**: `orders`, `deliveries`.

### Решение

| # | Подход | Плюсы | Минусы |
|---|--------|-------|--------|
| 1 | Два CTE (delivery_times, avg_deliveries) | Логично разделяет расчёт индивидуальных сроков и средних по дням. Просто и понятно. | Недостатков не выделено. |

```sql
WITH
    delivery_times AS
    (
    SELECT
        o.order_id,
        DATE(o.order_date) AS order_day,
        DATEDIFF(DATE(d.delivered_date), DATE(o.order_date)) AS delivery_days
    FROM
        orders o
    JOIN
        deliveries d
    ON
        o.order_id = d.order_id
    WHERE
        d.delivered_date IS NOT NULL
    ),
    avg_deliveries AS
    (
    SELECT
        order_day,
        AVG(delivery_days) AS avg_day_delivery
    FROM
        delivery_times 
    GROUP BY
        order_day
    )
SELECT
    dt.order_id,
    dt.order_day,
    dt.delivery_days AS delivery_days,
    ad.avg_day_delivery,
    dt.delivery_days - ad.avg_day_delivery AS delay_days,
    (dt.delivery_days > 5) * 1 AS sla_violation
FROM
    delivery_times dt
JOIN
    avg_deliveries ad
ON
    dt.order_day = ad.order_day
WHERE
    dt.delivery_days > ad.avg_day_delivery
ORDER BY
    order_day,
    order_id;
```

---

## Анализ успеваемости по экзаменам

**Условие**  
Для каждого экзамена рассчитать количество уникальных студентов, количество сдавших (лучшая попытка ≥ 60), процент сдавших (pass_percentage) и скользящее среднее этого процента по трём экзаменам (текущий, предыдущий, следующий) в порядке даты.

**Таблицы**: `exams`, `results`.

### Решение

| # | Подход | Плюсы | Минусы |
|---|--------|-------|--------|
| 1 | CTE с лучшими попытками + оконные функции | Все нужные метрики считаются в одном запросе. Скользящее среднее реализовано через окно ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING. | При отсутствии результатов для экзамена LEFT JOIN даст NULL, и pass_percentage станет NULL — это соответствует условию. |

```sql
WITH
    exam_students AS
    (
    SELECT
        exam_id,
        student_id,
        MAX(score) AS max_score
    FROM
        results
    GROUP BY
        exam_id,
        student_id
    )
SELECT
    e.exam_id,
    e.exam_name,
    e.exam_date,
    COUNT(DISTINCT es.student_id) AS total_students,
    SUM(CASE WHEN es.max_score >= 60 THEN 1 ELSE 0 END) AS passed_students,
    ROUND(
        SUM(CASE WHEN es.max_score >= 60 THEN 1 END) * 100.0 /
        COUNT(DISTINCT es.student_id), 2
    ) AS pass_percentage,
    ROUND(AVG(
        ROUND(SUM(CASE WHEN es.max_score >= 60 THEN 1 END) * 100.0 /
        COUNT(DISTINCT es.student_id), 2)
    ) OVER w, 2) AS sliding_pass_pct
FROM
    exams e
LEFT JOIN
    exam_students es
ON
    e.exam_id = es.exam_id
GROUP BY
    e.exam_id,
    e.exam_name,
    e.exam_date
WINDOW
    w AS
    (ORDER BY e.exam_date ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING)
ORDER BY
    exam_date;
```

---

## Рейтинг клиентов по сумме заказов за последние 90 дней

**Условие**  
CRM-отдел планирует кампанию персональных предложений. Для этого нужно построить рейтинг клиентов по сумме заказов за последние 90 дней относительно фиксированной даты отчёта (2025-08-12), чтобы результаты были воспроизводимыми. Рейтинг строится по каждой стране отдельно. Клиенты без заказов тоже должны попасть в результат — с нулевыми метриками и на последнем месте.

**Таблицы**: `customers`, `orders`.

### Решение

| # | Подход | Плюсы | Минусы |
|---|--------|-------|--------|
| 1 | Оконная функция в SELECT с WINDOW | Лаконично, сразу виден ранг. Использование `IFNULL` для клиентов без заказов. | При большом количестве клиентов может быть неэффективно из-за вычисления SUM в оконной функции, но для большинства случаев приемлемо. |

```sql
SELECT
    c.customer_id,
    c.name,
    c.country,
    COUNT(o.order_id) AS orders_count,
    SUM(o.amount) AS total_amount,
    AVG(o.amount) AS avg_check,
    RANK() OVER w AS rank_in_country
FROM
    customers c
LEFT JOIN
    orders o
ON
    c.customer_id = o.customer_id
    AND o.order_date BETWEEN '2025-08-12' - INTERVAL 90 DAY AND '2025-08-12'
GROUP BY
    c.customer_id,
    c.name,
    c.country
WINDOW
    w AS
    (PARTITION BY c.country ORDER BY IFNULL(SUM(o.amount), 0) DESC)
ORDER BY
    country,
    rank_in_country;
```

---

## Клиенты, купившие A и B в разные даты с разрывом ≥ 7 дней

**Условие**  
CRM-команда проверяет гипотезу о «отложенной кросс-продаже»: клиент сначала покупает товар 'A', а не раньше чем через неделю — товар 'B'. Нужен список клиентов, у которых покупки A и B были в разные даты с интервалом не менее 7 дней. Направление не важно (B может быть до A). Важно, чтобы это были разные заказы.

**Таблицы**: `orders`, `order_items`.

### Решение

| # | Подход | Плюсы | Минусы |
|---|--------|-------|--------|
| 1 | CTE с LAG для анализа соседних заказов | Использует оконные функции для сравнения последовательных заказов одного клиента. Чёткое условие на разницу дат и смену товаров. | Предполагает, что товары A и B не встречаются в одном заказе (иначе они будут в одной строке). Работает, если в заказе только один товар, иначе потребуется агрегация. |

```sql
WITH
    customer_orders_with_adjacent AS
    (
    SELECT
        o.customer_id,
        oi.product_id,
        o.order_date AS cur_date,
        LAG(o.order_date) OVER(PARTITION BY o.customer_id 
                               ORDER BY o.order_date, o.order_id) AS prev_date,
        LAG(oi.product_id) OVER(PARTITION BY o.customer_id 
                               ORDER BY o.order_date, o.order_id) AS prev_product_id
    FROM
        orders o
    JOIN
        order_items oi
    ON
        o.order_id = oi.order_id
    )
SELECT
    customer_id
FROM
    customer_orders_with_adjacent
WHERE
    cur_date >= prev_date + INTERVAL 7 DAY
    AND (product_id = 'A' AND prev_product_id = 'B' 
         OR product_id = 'B' AND prev_product_id = 'A') 
GROUP BY
    customer_id
ORDER BY
    customer_id;
```

---

## Рейтинг задач по сложности и интересности для Авито

Ниже представлена субъективная оценка сложности каждой задачи (с точки зрения используемых конструкций SQL) и её потенциальная бизнес-ценность для платформы вроде Авито (e-commerce, классифайд). Рейтинг построен от наиболее сложных/интересных к менее сложным.

| Ранг | Задача | Обоснование |
|:----:|--------|-------------|
| 1 | **Герой-товар по отделу с учётом возвратов** | Требует учёта возвратов, агрегации, оконных функций, сложного tie-break. Высокая сложность, критично для e-commerce (точный расчёт выручки). |
| 2 | **Топ-товары в «премиальных» категориях** | Многоступенчатая логика, сравнение со средним, оконные функции, топ-1 с дубликатами. Важно для маркетинга и ассортиментной политики. |
| 3 | **Потенциальный дефицит на складе** | Работа с временными рядами, недельная агрегация, условие риска. Актуально для логистики и управления запасами. |
| 4 | **Ежедневный CTR с учётом повторных кликов и регистраций** | Учёт уникальности, временные окна, расчёт маркетинговой метрики. Классическая задача для рекламных платформ. |
| 5 | **Клиенты, купившие A и B в разные даты с разрывом ≥ 7 дней** | Использование LAG для анализа последовательности покупок, интересно для кросс-продаж и персонализации. |
| 6 | **Анализ успеваемости по экзаменам** | Оконные функции, скользящее среднее, работа с лучшими попытками. Технически сложная, хотя тематика не e-commerce. |
| 7 | **Быстрая активация клиентов до марта** | Несколько решений, анализ когорт, условие на третий заказ. Полезно для CRM и удержания клиентов. |
| 8 | **Активнее среднего в регионе «Сибирь»** | Расчёт среднего с учётом нулевых пользователей, проценты. Хорошо для регионального анализа активности. |
| 9 | **Сегментация клиентов по активности и выручке** | CASE с приоритетами, флаги. Полезно, но технически просто. |
| 10 | **Раздражённые клиенты и tricky-обращения** | Использование LAG, условия на разрывы. Средняя сложность, нишевая задача для поддержки. |
| 11 | **Заказы с нарушением сроков доставки** | Простые агрегаты, разница дат. Логистика, но технически несложно. |
| 12 | **Рейтинг клиентов по сумме заказов за последние 90 дней** | Оконная функция, но без особых хитростей. Базовая задача для ранжирования. |

---

## Общие выводы

- **Многоступенчатые CTE** обеспечивают максимальную прозрачность и удобство отладки, но могут быть избыточными и менее производительными. Они идеальны для обучения и сложных расчётов, где важна пошаговая проверка.
- **Оконные функции** позволяют сильно сократить код и часто выполняются эффективнее, однако требуют хорошего понимания порядка вычислений и могут быть сложны для новичков.
- **Компактные запросы с единственным CTE** представляют собой золотую середину: они лаконичны, но сохраняют логическую структуру.
- При работе с возвратами или другими связанными сущностями всегда нужно учитывать возможность множественных записей. В таких случаях необходима предварительная агрегация или использование `DISTINCT`, иначе результаты будут искажены.
- Важно проверять условия на граничные значения (NULL, деление на ноль) — в решениях активно используется `COALESCE` и `NULLIF`.

Выбор подхода зависит от конкретной задачи, объёма данных и требований к читаемости кода. Данный репозиторий демонстрирует, как одну и ту же бизнес‑задачу можно решить разными способами, и помогает оценить их сильные и слабые стороны.

---

**Контакты**  
Если у вас есть вопросы или предложения, смело создавайте Issue или Pull Request.
```
