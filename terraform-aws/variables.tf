variable "ami_id" {
  description = "The AMI ID to use for EC2 instances"
  type        = string
}

variable "key_name" {
  description = "The name of the SSH key pair to use for EC2 instances"
  type        = string
  default     = "MIIEogIBAAKCAQEAq5YNFHJD0iVYpYo7ouwomPIcSmtpxWcrrewhHps9vBtvxI3T
                ZGCND/OafT4313JYs65ClVcFcAnynFp33rc9l9atOFZf+CCLH1LZytFr9fK0uPC4
                vXyOVVBAxJ/DJxq23nJmh9fyA+pYfEwVqjt6lG8eLWdAsG45Y6Nn0ha66sOnxOyS
                oi/oqiVo8tAaNkVW8npCNexekzagCEVe0akgj+bvsUY6D7PhdLsbQycp+ky1Qj5W
                AYEBY0rrKKcRagd+uANqCTImN/EJlMaZEkcy7kRYO0cntrWIPCQWKVLyUoPwAU5S
                LCJt87rpcTtB2WNMeqr/jKhI8auMecmo4+L6WwIDAQABAoIBAEoWrmsns5UnvxEr
                kPiWVClGUWTo4HD2TWv5y4s1qQByMF6EhsSO1BoRK6HsnZwPqbFcCzAEtTpdcT6F
                6cBnCtdnQMBUu3eeRbQuyA/FQYKfW92HQ52+DI9V6cF84n0eEdwXNaJuYffC1pZF
                HA23RQSWvkcIkGweC/Wz9v5GBkubnujmMy3HlF+1KaJM8OMmg57zfGzRZackwc0V
                pC/V2/PtgbqqdP3QSWNGUnRONmUswccUafSSTYB2cIGi6H9ICyxiD4052Jfmp+Jp
                m83CRHHJu9MT/2e9JSAkkA/7DV+37wttg/4Rwt00ySrGEdl1OLGnpH5RpEhe0hb1
                /tDJvBECgYEA1l19W/MvqN+ahaHVPkaIzoYRfhHlpCZrkh6lrgVPypcNU91RQtWm
                8Nn0ogf9rI3xLaA0+76Fl0HP7kOs0gYyO6NlBYaXQvKp4sStjRpiQnto3RmJ2g3O
                Dr4wBqNwGe0WhinLHmrDRy888YE+t2sdf6ndgVH3Iy8wr7VgxmmS2Z0CgYEAzOmI
                QPqRt3vcD04+HmRMRqjid2MHOwa1ObexFbC9B/AwNKQikEY3NnK4PJhj2iNb/drm
                zatAJljPT7Ah+p2+jQ9AggIqzWX2AXofr1rvgPnrzfR0M+Eh+uHwXiaiOO8tRC18
                cMmNURQkKzepDaWE+t90gnaiJJC//fz8ePgzPlcCgYBsNu3dyTo6Cgc7hqLbuUe+
                2jdiaS6AW1TagtYor03Ee9SijYtELg8Eb0LruRT6Uv15hvK4U0mlPff10/weWjpp
                mOaaj4M0rMPOUnM6VCNeZGZfl1Db3zQyhRhBgahJrkI8oESFqmfCO7qMQC6k8VIG
                7H2Blxsni98MFIgyIYGckQKBgBDuTtLHoViexE0DcwCB2wePlr60kPlgkYLGWbxo
                EQZh4ynGUhDHrHI0QmLHWKDCgSxVdPKTbsZ8WgzEidoyRHdVRkg3s5+rCAuRMqMD
                iXyHqeMnip5qwKsBFiJBYPABWyUh+QE8tg938ZEclTxKa9Vqty68bKNGzoZG6/l2
                0I0bAoGAa6sTrAWSG1+oRMYbtuqWNRqEq7a7sfp5ySpp2N6Yq7/GjQtmtG/YzeKB
                TkdGPMTot6e5i7zunWIYM3l7bXgKXNJ7r/krSThA69RoQw/+fyvAWl/D/zZP5Az6
                Hp4Z7wBVigHJOKoVOTgNU6f6mmi1TserMBQH8Pqo2VGRX8/GdJQ="
}

variable "region" {
  description = "The AWS region to deploy resources"
  type        = string
  default     = "us-east-1"
}

variable "vpc_cidr" {
  description = "CIDR block for the VPC"
  type        = string
  default     = "10.0.0.0/16"
}

variable "subnet_cidr" {
  description = "CIDR block for the public subnet"
  type        = string
  default     = "10.0.1.0/24"
}

variable "crawler_count" {
  description = "Number of crawler nodes to create"
  type        = number
  default     = 3
}

variable "indexer_count" {
  description = "Number of indexer nodes to create"
  type        = number
  default     = 2
}

variable "instance_type" {
  description = "EC2 instance type for all nodes"
  type        = string
  default     = "t2.micro"
}

variable "opensearch_instance_type" {
  description = "OpenSearch instance type"
  type        = string
  default     = "t3.small.search"
}

variable "opensearch_instance_count" {
  description = "Number of OpenSearch instances"
  type        = number
  default     = 1
}

variable "opensearch_volume_size" {
  description = "Size of the OpenSearch EBS volume in GB"
  type        = number
  default     = 10
}
