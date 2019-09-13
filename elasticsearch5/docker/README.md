
# Amazon EC2

1. Create a tar of the Elasticsearhc image: `docker save -o /tmp/docker_elasticsearch.tar docker_elasticsearch`
1. Create x number of EC2 instances based on Amazon Linux
1. Copy image to instances `scp /tmp/docker_elasticsearch.tar ec2-user@<ip address>:`
1. `ssh ec2-user@<ip address>`
1. Install docker

     sudo su -
     yum install -y docker
     systemctl enable docker.service
     systemctl start docker.service
     sysctl -w vm.max_map_count=262144
     ulimit -n 90000

1. Load docker image `docker load --input /home/ec2-user/docker_elasticsearch.tar`
1. Start Elasticsearch `docker run --network host -d --name elasticsearch -e "discovery.zen.ping.unicast.hosts=<comma separated list of IP addresses>" docker_elasticsearch`