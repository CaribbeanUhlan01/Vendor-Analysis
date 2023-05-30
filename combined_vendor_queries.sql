-- joins for mu_table and mi_table queries 


-- Let's join the number of inspections per state
CREATE VIEW total_inspections_per_year AS 
SELECT mu_insps_per_year.order_year, mu_insps_per_year.sum_insps_year + mi_insps_per_year.sum_insps_year AS mi_mu_insps_per_year
FROM mu_insps_per_year
JOIN mi_insps_per_year ON mu_insps_per_year.order_year = mi_insps_per_year.order_year
ORDER BY order_year


-- Total inspections per weekday
CREATE VIEW total_inspections_per_weekday AS
SELECT mu_insps_per_weekday.weekday_name, mu_insps_per_weekday.sum_insps_day + mi_insps_per_weekday.sum_insps_day AS mi_mu_insps_per_day
FROM mu_insps_per_weekday
JOIN mi_insps_per_weekday ON mu_insps_per_weekday.weekday_name = mi_insps_per_weekday.weekday_name
ORDER BY 
	CASE mu_insps_per_weekday.weekday_name
		WHEN 'Sunday' THEN 7
        WHEN 'Monday' THEN 1
        WHEN 'Tuesday' THEN 2
        WHEN 'Wednesday' THEN 3 
        WHEN 'Thursday' THEN 4
        WHEN 'Friday' THEN 5
        WHEN 'Saturday' THEN 6 
	END
OFFSET 0 ROWS;



-- Total inspections per month
CREATE VIEW total_inspections_per_month AS 
SELECT mu_insps_per_month.month, mu_insps_per_month.sum_insps_month + mi_insps_per_month.sum_insps_month AS mi_mu_insps_per_month
FROM mu_insps_per_month
JOIN mi_insps_per_month ON mu_insps_per_month.month = mi_insps_per_month.month


-- Total inspections per state
CREATE VIEW total_inspections_per_state AS
SELECT mu_insps_per_state.State, mu_insps_per_state.sum_insps_state + mi_insps_per_state.sum_insps_state AS mi_mu_insps_per_month
FROM mu_insps_per_state
JOIN mi_insps_per_state ON mu_insps_per_state.State = mi_insps_per_state.State
ORDER BY mi_mu_insps_per_month DESC OFFSET 0 ROWS;


-- Disposition code distribution 
CREATE VIEW total_disp_code_distribution AS
SELECT mu_disp_code_distribution.Disposition_Code, mu_disp_code_distribution.sum_disp_code + mi_disp_code_distribution.sum_disp_code AS mi_mu_disp_distribution
FROM mu_disp_code_distribution
JOIN mi_disp_code_distribution ON mu_disp_code_distribution.Disposition_Code = mi_disp_code_distribution.Disposition_Code
ORDER BY mi_mu_disp_distribution DESC OFFSET 0 ROWS;


-- To improve query fetch times let's create an index
ALTER VIEW appended_tla_table AS
SELECT mu_table.state, mu_table.square_footage
FROM mu_table
UNION ALL -- Inlcuding duplicate Square footages 
SELECT mi_table.state, mi_table.tla
FROM mi_table

ALTER VIEW total_avg_living_are_per_state AS
SELECT state, CAST(AVG(square_footage) AS DECIMAL(16,2)) AS avg_square_footage
FROM appended_tla_table
GROUP BY State
ORDER BY avg_square_footage DESC OFFSET 0 ROWS;


-- Creating combined variation table
CREATE VIEW appended_var_table AS
SELECT CAST((Prcnt_Val) / 100 AS "DECIMAL"(16,2)) AS average_variation
FROM mu_table
UNION ALL 
SELECT CAST((VAR) AS "DECIMAL"(16,2)) AS average_variation
FROM mi_table;

CREATE VIEW total_avg_variation AS
SELECT CAST(AVG(average_variation) / 1 AS "DECIMAL"(16,2)) AS mi_mu_average_variation
FROM appended_var_table


-- Combined variation per state
CREATE VIEW appended_var_per_state_table AS
SELECT State, CAST((Prcnt_Val) / 100 AS "DECIMAL"(16,2)) AS average_variation_state,
average_variation - CAST((Prcnt_Val) / 100 AS "DECIMAL"(16,2)) AS diff_from_overall_avg_val
FROM mu_table, mu_average_overall_variation
UNION ALL 
SELECT State, CAST((VAR) AS "DECIMAL"(16,2)) AS average_variation_state,
average_variation - CAST((VAR) AS "DECIMAL"(16,2)) AS diff_from_overall_avg_val
FROM mi_table, mi_average_overall_variation

CREATE VIEW total_variation_per_state AS 
SELECT State, CAST(AVG(average_variation_state) / 1 AS "DECIMAL"(16,2)) AS mi_mu_avg_var_per_state,  
CAST(AVG(average_variation_state) / 1 AS "DECIMAL"(16,2)) - mi_mu_average_variation AS mi_mu_diff_from_overall_var
FROM appended_var_per_state_table, total_avg_variation
GROUP BY State, mi_mu_average_variation
ORDER BY mi_mu_avg_var_per_state DESC OFFSET 0 ROWS;



-- Average variation per disposition code
CREATE VIEW appended_var_per_disp_table AS
SELECT UW_Action, CAST((Prcnt_Val) / 100 AS "DECIMAL"(16,2)) AS variation_per_disp
FROM mu_table, mu_average_overall_variation
UNION ALL
SELECT Disposition_Code, CAST((VAR) AS "DECIMAL"(16,2)) AS variation_per_disp
FROM mi_table, mi_average_overall_variation

CREATE VIEW total_avg_variation_per_disp AS 
SELECT UW_Action, CAST(AVG(variation_per_disp) / 1 AS "DECIMAL"(16,2)) AS mi_mu_var_per_disp
FROM appended_var_per_disp_table
GROUP BY UW_Action
ORDER BY mi_mu_var_per_disp DESC OFFSET 0 ROWS




-- Looking at STATE average completion times
CREATE VIEW appended_total_completion_times AS
SELECT State, (DATEDIFF(day, Ordered, Completed)) AS completion_time_days_state,
(DATEDIFF(day, Ordered, Completed)) - completion_time_days AS variance_from_national_average
FROM mu_table, mu_avg_completion_days
UNION ALL
SELECT State, (DATEDIFF(day, Order_Date, Complete_Date)) AS completion_time_days_state,
(DATEDIFF(day, Order_Date, Complete_Date)) - completion_time_days AS variance_from_national_average
FROM mi_table, avg_completion_days

SELECT State, AVG(completion_time_days_state) AS mi_mu_avg_completion_time
FROM appended_total_completion_times
GROUP BY State
ORDER BY mi_mu_avg_completion_time DESC OFFSET 0 ROWS;

-- Average Coverage A per state
CREATE VIEW appended_cov_per_state AS
SELECT State, Order_Year, Coverage_A_In AS avg_coverage 
FROM mu_table
UNION ALL
SELECT State, Order_Year, Coverage AS avg_coverage
FROM mi_table

CREATE VIEW total_coverage_per_state AS
SELECT state, Order_year, CAST(AVG(avg_coverage) AS BIGINT) AS mu_mi_avg_coverage
FROM appended_cov_per_state
WHERE avg_coverage IS NOT NULL
GROUP BY state, Order_year
ORDER BY state, Order_year OFFSET 0 ROWS


CREATE VIEW total_coverage_per_state_only AS
SELECT state, CAST(AVG(avg_coverage) AS BIGINT) AS mu_mi_avg_coverage
FROM appended_cov_per_state
WHERE avg_coverage IS NOT NULL
GROUP BY state
ORDER BY state OFFSET 0 ROWS












-- Total inspections per year
SELECT * FROM total_inspections_per_year

-- Total inspections per weekday 
SELECT * FROM total_inspections_per_weekday

-- Total inspections per month
SELECT * FROM total_inspections_per_month

-- Total inspections per state 
SELECT * FROM total_inspections_per_state

-- Total disposition code distribution 
SELECT * FROM total_disp_code_distribution

-- Total living area per state
SELECT * FROM total_avg_living_are_per_state

-- Total national average variation
SELECT * FROM total_avg_variation

-- Total variation by state
SELECT * FROM total_variation_per_state

-- Total avg variation per disp code
SELECT * FROM total_avg_variation_per_disp

-- Total coverage per state and year
SELECT * FROM total_coverage_per_state


-- Total coverage per state only
SELECT * FROM total_coverage_per_state_only
