#!/usr/bin/env bash

ssh-keygen -q -N "" -t rsa -f ~/.ssh/id_rsa
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
echo "    StrictHostKeyChecking no                     " | sudo tee -a /etc/ssh/ssh_config
sudo service ssh restart
