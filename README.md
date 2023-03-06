# Alert_system
An automated alert system implemented on Airflow to check the deviation of the metric value in the current 15-minute from the value in the same 15-minute a day ago.

If an abnormal value is detected, an alert is sent to the TG chat - a message with the following information: metric, its value, deviation value.
Additional information can be added to the message that will help in investigating the causes of the anomaly, for example, it can be a graph, links to a dashboard/chart in the BI system.





