import configparser
import boto3
import json
import time


def create_or_get_iam_role(iam_client, DWH_IAM_ROLE_NAME):
    """
    Create an IAM role, or retreive it if it already exists.

    Args:
        iam_client,
        DWH_IAM_ROLE_NAME 
    Returns:
        The IAM role
    """
    try:
        print("Creating a new IAM Role")
        dwhRole = iam_client.create_role(
            Path="/",
            RoleName=DWH_IAM_ROLE_NAME,
            Description="Allows Redshift clusters to call AWS services on your behalf.",
            AssumeRolePolicyDocument=json.dumps(
                {
                    "Statement": [
                        {
                            "Action": "sts:AssumeRole",
                            "Effect": "Allow",
                            "Principal": {"Service": "redshift.amazonaws.com"},
                        }
                    ],
                    "Version": "2012-10-17",
                }
            ),
        )
    except Exception as e:
        print(e)
        print("Getting IAM Role")
        dwhRole = iam_client.get_role(RoleName=DWH_IAM_ROLE_NAME)
    return dwhRole


def create_redshift_cluster(
    redshift_client,
    roleArn,
    DWH_CLUSTER_TYPE,
    DWH_NODE_TYPE,
    DWH_NUM_NODES,
    DWH_CLUSTER_IDENTIFIER,
    DB_NAME,
    DB_USER,
    DB_PASSWORD,
):
    """
    Create the Redshift cluster.

    Args:
        redshift_client
        roleArn,
        DWH_CLUSTER_TYPE,
        DWH_NODE_TYPE,
        DWH_NUM_NODES,
        DWH_CLUSTER_IDENTIFIER,
        DB_NAME,
        DB_USER,
        DB_PASSWORD
    Returns:
        None
    """
    try:
        response = redshift_client.create_cluster(
            # HW
            ClusterType=DWH_CLUSTER_TYPE,
            NodeType=DWH_NODE_TYPE,
            NumberOfNodes=int(DWH_NUM_NODES),
            # Identifiers & Credentials
            DBName=DB_NAME,
            ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
            MasterUsername=DB_USER,
            MasterUserPassword=DB_PASSWORD,
            # Roles (for s3 access)
            IamRoles=[roleArn],
            PubliclyAccessible=True,
        )
        print(
            "Http status code response from Redshift Cluster: ",
            response["ResponseMetadata"]["HTTPStatusCode"],
        )
    except Exception as e:
        print(e)


def update_config_file(redshift_client, DWH_CLUSTER_IDENTIFIER, sleep_seconds=60):
    """
    Update config file with the cluster endpoint and IAM ARN values after the cluster has become available.
    It can take 5+ minutes for the cluster to spin up and become available.

    Args:
        redshift_client, 
        DWH_CLUSTER_IDENTIFIER, 
        sleep_seconds
    Returns:
        None
    """

    print(f"Checking every {sleep_seconds} seconds for cluster to be available")
    print("\tCheck Number,\tCluster Status")
    myClusterProps = redshift_client.describe_clusters(
        ClusterIdentifier=DWH_CLUSTER_IDENTIFIER
    )["Clusters"][0]

    i = 0
    while myClusterProps["ClusterStatus"] != "available":
        print(f"\t{i},\t\t{myClusterProps['ClusterStatus']}")
        i += 1
        time.sleep(sleep_seconds)
        myClusterProps = redshift_client.describe_clusters(
            ClusterIdentifier=DWH_CLUSTER_IDENTIFIER
        )["Clusters"][0]

    print("Cluster Status = ", myClusterProps["ClusterStatus"])

    DWH_ENDPOINT = myClusterProps["Endpoint"]["Address"]
    DWH_ROLE_ARN = myClusterProps["IamRoles"][0]["IamRoleArn"]
    print("DWH_ENDPOINT = ", DWH_ENDPOINT)
    print("DWH_ROLE_ARN = ", DWH_ROLE_ARN)

    print("Writing cluster endpoint and IAM role ARN to dwh.cfg")
    config = configparser.ConfigParser()

    # Read in the current config file
    with open("dwh.cfg") as f:
        config.read_file(f)

    # Update the HOST and ARN values
    config.set("CLUSTER", "HOST", myClusterProps["Endpoint"]["Address"])
    config.set("IAM_ROLE", "ARN", myClusterProps["IamRoles"][0]["IamRoleArn"])

    # Write out the new config file
    with open("dwh.cfg", "w+") as f:
        config.write(f)


def main():
    # Parse config file
    config = configparser.ConfigParser()
    config.read("dwh.cfg")

    KEY = config.get("AWS", "KEY")
    SECRET = config.get("AWS", "SECRET")

    DWH_CLUSTER_TYPE = config.get("DWH", "DWH_CLUSTER_TYPE")
    DWH_NUM_NODES = config.get("DWH", "DWH_NUM_NODES")
    DWH_NODE_TYPE = config.get("DWH", "DWH_NODE_TYPE")
    DWH_IAM_ROLE_NAME = config.get("DWH", "DWH_IAM_ROLE_NAME")
    DWH_CLUSTER_IDENTIFIER = config.get("DWH", "DWH_CLUSTER_IDENTIFIER")
    REGION_NAME = config.get("DWH", "REGION_NAME")

    DB_NAME = config.get("CLUSTER", "DB_NAME")
    DB_USER = config.get("CLUSTER", "DB_USER")
    DB_PASSWORD = config.get("CLUSTER", "DB_PASSWORD")

    # Create clients for IAM role and Redshift
    iam = boto3.client(
        "iam",
        region_name=REGION_NAME,
        aws_access_key_id=KEY,
        aws_secret_access_key=SECRET,
    )
    redshift = boto3.client(
        "redshift",
        region_name=REGION_NAME,
        aws_access_key_id=KEY,
        aws_secret_access_key=SECRET,
    )

    # Create an IAM Role that makes Redshift able to access S3 bucket (ReadOnly)
    create_or_get_iam_role(iam, DWH_IAM_ROLE_NAME)

    # Attach S3 read only access policy to IAM role
    response = iam.attach_role_policy(
        RoleName=DWH_IAM_ROLE_NAME,
        PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess",
    )["ResponseMetadata"]["HTTPStatusCode"]
    print(response)

    # Get ARN for iam role
    roleArn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)["Role"]["Arn"]
    print("roleArn:", roleArn)

    # Create the cluster
    create_redshift_cluster(
        redshift,
        roleArn,
        DWH_CLUSTER_TYPE,
        DWH_NODE_TYPE,
        DWH_NUM_NODES,
        DWH_CLUSTER_IDENTIFIER,
        DB_NAME,
        DB_USER,
        DB_PASSWORD,
    )

    # Wait for the cluster to spin up and update the config file HOST and ARN values
    update_config_file(redshift, DWH_CLUSTER_IDENTIFIER, sleep_seconds=30)


if __name__ == "__main__":
    import sys
    import os

    os.chdir(sys.path[0])  # Change path to that of this python file

    main()
