#! /bin/bash

vagrant up
vagrant ssh nodeA -c "./after_startup.sh && ./start-master.sh"
vagrant ssh nodeB -c "./after_startup.sh && ./start-slave.sh"
