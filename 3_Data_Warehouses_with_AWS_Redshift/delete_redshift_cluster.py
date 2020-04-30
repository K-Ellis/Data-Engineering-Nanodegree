if __name__ == "__main__":
    import boto3
    import configparser
    import sys
    import os
    import time

    os.chdir(sys.path[0])

    config = configparser.ConfigParser()
    config.read("dwh.cfg")

    KEY = config.get("AWS", "KEY")
    SECRET = config.get("AWS", "SECRET")
    DWH_IAM_ROLE_NAME = config.get("DWH", "DWH_IAM_ROLE_NAME")
    DWH_CLUSTER_IDENTIFIER = config.get("DWH", "DWH_CLUSTER_IDENTIFIER")
    REGION_NAME = config.get("DWH", "REGION_NAME")

    redshift = boto3.client(
        "redshift",
        region_name=REGION_NAME,
        aws_access_key_id=KEY,
        aws_secret_access_key=SECRET,
    )

    try:
        redshift.delete_cluster(
            ClusterIdentifier=DWH_CLUSTER_IDENTIFIER, SkipFinalClusterSnapshot=True
        )

        myClusterProps = redshift.describe_clusters(
            ClusterIdentifier=DWH_CLUSTER_IDENTIFIER
        )["Clusters"][0]
        print("Cluster Status:", myClusterProps["ClusterStatus"])

        # It can take about 3 minutes to delete the cluster
        # at which point the try will error saying the cluster can not be found and the roles can be deleted.
        i = 0
        while myClusterProps["ClusterStatus"] == "deleting":
            print(f"\t{i},\t{myClusterProps['ClusterStatus']}")
            i += 1
            time.sleep(30)
            myClusterProps = redshift.describe_clusters(
                ClusterIdentifier=DWH_CLUSTER_IDENTIFIER
            )["Clusters"][0]
    except Exception as e:
        print(e)

    iam = boto3.client(
        "iam",
        region_name=REGION_NAME,
        aws_access_key_id=KEY,
        aws_secret_access_key=SECRET,
    )
    iam.detach_role_policy(
        RoleName=DWH_IAM_ROLE_NAME,
        PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess",
    )
    iam.delete_role(RoleName=DWH_IAM_ROLE_NAME)
