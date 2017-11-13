# worker pool


thread safe worker pool with queuing of function jobs for controlled resource allocation

features:
* thread safe
* unlimited queue for functions
* limited resources - limit workers
* each job function run in a go routine

### job control
* able to stop job and get notification when stopped
* keep correlation between job and pool
