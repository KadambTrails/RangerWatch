#!/bin/bash

sudo dnf update -y
sudo dnf install git docker -y

sudo systemctl start docker
sudo systemctl enable docker

sudo usermod -aG docker ec2-user