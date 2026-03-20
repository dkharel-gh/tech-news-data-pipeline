# tech-news-data-pipeline
simple etl pipeline focusing on tech news data from hackernews, techcrunch and github trending (to start with)


1. I need to create multiple tables for "top_fifty", "new_stories" and such.
    - needs more thought
2. for now, the 10 minute cron task should do the job.
    - the db will start to grow large very quickly so get on top of managing the db system.
* Welcome to your data engineering role.

--------------------------------------


TO DO:
    1. make sure the volume bind is working correctly.
    2. make different docker-compose-etl.yml for docker-compose-local.yml.
        - so the volume mount doens't cause issue between diff devices.
    3.

--------------------------------------

Step 1: need a db container running.
    - postgres image can be pulled from dockerhub.
    - the etl service depends on running postgres db container.
    - the etl service needs a Dockerfile to build the image.
    - it also needs a python module that will run in the etl.

    - setup the etl/etl folder as the module
    - fixed the services in docker compose to run the db container and the etl-runner (from dockerfile).

Step 2: need a python script to download the data
    - for now let's just print helloworld
    - used the hn_test.py script to download 50 topics at once
    - made it display the polls and keep it running.

Step 3: need a python script to load the data to the db container.
    - 