# Pulls lineage data from tableau server to understand tableau's servers

from tableau_api_lib import TableauServerConnection
import pandas as pd
import psycopg2

# Tableau Server credentials
tableau_server_config = {
    'tableau_prod': {
        'server': 'http://test',
        'api_version': '3.10',
        'username': 'tab',
        'password': '123456',
        'site_name': 'Site',
        'site_url': 'Site'
    }
}

conn = TableauServerConnection(tableau_server_config, 'tableau_prod')


# connect and query graphQL data from tableau server
def pull_graphql_data(graphql):
    conn.sign_in()
    connection = conn.metadata_graphql_query(query=graphql)
    return connection


def pull_postgres_data():
    """# Tableau postgres db credentials"""
    tableau_postgres_config = psycopg2.connect(
        host="tableau",
        database="workgroup",
        user="readonly",
        password="password",
        port="8080")

    sql = """
            SELECT DISTINCT background_jobs.title,
                            schedules.name
            FROM background_jobs
                 LEFT JOIN tasks AS task ON background_jobs.correlation_id = task.id
                 LEFT JOIN subscriptions ON task.obj_id = subscriptions.id AND task.type::text = 'SingleSubscriptionTask'::text
                 LEFT JOIN _schedules AS schedules ON task.schedule_id = schedules.id
            WHERE CAST(background_jobs.created_at AS DATE) >= current_date - 1
        """

    data = pd.read_sql(sql, tableau_postgres_config)
    return data


def merge_data():
    graphql_query_flows = """
    {
      flows {
        name
        projectName
        owner{
          name
        }
        downstreamWorkbooks {
          name
        }
      }
    }
    """

    # Convert graphQL data into json
    flow_json = pull_graphql_data(graphql_query_flows).json()

    # Import json data into dataframe
    flow_normalized_data = pd.json_normalize(flow_json['data']['flows']
                                             , meta=['name', 'projectName', ['owner', 'name']]
                                             , meta_prefix='flows.'
                                             , record_prefix='downstreamWorkbooks.'
                                             )

    # Merge graphql data with postgres data to bring through schedule assignment by flow name
    full_data = flow_normalized_data.merge(pull_postgres_data(), left_on='name', right_on='title')

    # Write to excel to view data
    full_data.to_excel("flows.xlsx")

    conn.sign_out()


if __name__ == "__main__":
    merge_data()
