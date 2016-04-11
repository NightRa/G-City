#! /bin/bash

vagrant up
vagrant ssh master -c "./after_startup.sh"
vagrant ssh slave -c "./after_startup.sh"
