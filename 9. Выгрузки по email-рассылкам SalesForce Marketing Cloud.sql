--Выгрузка статистики о компаниях для раздела {month}_Email

--Поиск нужных JobID конкретной кампании для фильтрации таблиц с действиями (аналог Action из SF) 

--Пример для 'L-05'

SELECT EmailName, j.JobID,
count(o.SubscriberID) over (partition by j.JobID)
FROM _Job j
Left join _Open o on j.JobID = o.JobID
Where EmailName  = 'L-05'

--Находим те  JobID, где count(o.SubscriberID) over (partition by j.JobID) не равен 0
--пример: 542933, 543793

--Далее по этим JobID выгружаем действия

SELECT
AccountID,
OYBAccountID,
JobID,
ListID,
BatchID,
SubscriberID,
SubscriberKey,
EventDate,
Domain,
IsUnique,
TriggererSendDefinitionObjectID,
TriggeredSendCustomerKey
FROM _Open
Where jobid in (542933, 543793) and IsUnique = 1

SELECT
AccountID,
OYBAccountID,
JobID,
ListID,
BatchID,
SubscriberID,
SubscriberKey,
EventDate,
Domain,
IsUnique,
TriggererSendDefinitionObjectID,
TriggeredSendCustomerKey
FROM _click
Where jobid in (542933, 543793) and IsUnique = 1


SELECT
AccountID,
OYBAccountID,
JobID,
ListID,
BatchID,
SubscriberID,
SubscriberKey,
EventDate,
Domain,
TriggererSendDefinitionObjectID,
TriggeredSendCustomerKey
FROM _sent
Where jobid in (542933, 543793)

--этот показатель выгружается для расчета deliv = sent-bounce
SELECT
AccountID,
OYBAccountID,
JobID,
ListID,
BatchID,
SubscriberID,
SubscriberKey,
EventDate,
Domain,
TriggererSendDefinitionObjectID,
TriggeredSendCustomerKey
FROM _bounce
Where jobid in (542933, 543793)



SELECT
AccountID,
OYBAccountID,
JobID,
ListID,
BatchID,
SubscriberID,
SubscriberKey,
EventDate,
Domain
FROM _Unsubscribe
Where jobid in (542933, 543793)


--дополнительно фильтрация по сегменту GONE. Выбираем NOT GONE путем фильтрации SubscriberID, которые совершили открытия в период более 150 дней от даты отчета

SELECT
AccountID,
OYBAccountID,
JobID,
ListID,
BatchID,
SubscriberID,
SubscriberKey,
EventDate,
Domain,
TriggererSendDefinitionObjectID,
TriggeredSendCustomerKey
FROM _Open
Where EventDate >= 'Jul 28 2024 0:00AM'

--Далее эти SubscriberID через ВПР сравниваем со списком SubscriberID в Sent, и находим sent для Gone  (open - not gone = gone)
