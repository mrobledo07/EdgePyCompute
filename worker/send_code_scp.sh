scp -i ~/Downloads/worker1.pem -r src package* .env ec2-user@ec2-13-50-100-35.eu-north-1.compute.amazonaws.com:/home/ec2-user

scp -i ~/Downloads/worker2.pem -r src package* .env ec2-user@ec2-13-61-17-231.eu-north-1.compute.amazonaws.com:/home/ec2-user

scp -i ~/Downloads/worker3.pem -r src package* .env ec2-user@ec2-51-20-44-33.eu-north-1.compute.amazonaws.com:/home/ec2-user
