import pandas as pd
from jira.client import JIRA


class JiraFacilitator:

    def __init__(self, rsa_key, access_token, access_secret):

        self.rsa_key = rsa_key
        self.access_token = access_token
        self.access_secret = access_secret
        self.JIRA_SERVER = 'https://jira.wehkampgroep.nl'
        self.CONSUMER_KEY = 'OauthKey'
        self.jira_instance = self.connect()

    def all_issues(self, project):
        return self.jira_instance.search_issues('project = "{}"'.format(project), maxResults=False)

    def connect(self):


        # Now you can use the access tokens with the JIRA client.
        jira = JIRA(options={'server': self.JIRA_SERVER}, oauth={
            'access_token': self.access_token,
            'access_token_secret': self.access_secret,
            'consumer_key': self.CONSUMER_KEY,
            'key_cert': self.rsa_key})

        return jira

    def post_issues(self, project, project_key, pdf, **kwargs):

        #   for col in kwargs.keys():
        #     assert col in df.columns, 'Column {} should be in the dataframe'.format(col)

        total = len(pdf)

        for index, row in pdf.iterrows():

            post_dict_ = {"project": {"name": project, "key": project_key}}

            for key, value in kwargs.items():

                if not key.startswith('col_'):
                    post_dict_[key] = value
                else:
                    key_ = key[4:]
                    post_dict_[key_] = row[value]

            response = self.jira_instance.create_issue(post_dict_)
            print('Alerts posted {} / {}. : {}'.format(index + 1, total, response))

        return None

    def move_issues(self, pdf, issue_key_column, move_to: str, fields_map):

        assert issue_key_column in pdf.columns, f"Column {issue_key_column} is not in the dataframe!"

        for index, row in pdf.iterrows():
            fields = {}
            for key, value in fields_map.items():
                if not key.startswith('col_'):
                    fields[key] = value
                else:
                    key_ = key[4:]
                    fields[key_] = row[value]

            response = self.jira_instance.transition_issue(row[issue_key_column], move_to, fields)
            print('Ticket {} is moved to {}'.format(row[issue_key_column], move_to))

        return None

    def delete_issue(self, issue_str, delete_subtasks: bool = True):
        return self.jira_instance.issue(issue_str).delete(deleteSubtasks=delete_subtasks)

    def batch_delete_issues(self, issues_to_delete, issue_key_column: str, delete_subtasks: bool = True):

        assert issue_key_column in issues_to_delete.columns, 'alert_key is not in dataframe!'

        if len(issues_to_delete) != 0:
            print('Found issues to be deleted')
            get_issues_list = issues_to_delete.select('alert_key').distinct().rdd.flatMap(list).collect()
            for issue in get_issues_list:

                print('Deleting {}'.format(issue))
                self.delete_issue(issue, delete_subtasks=delete_subtasks)



            print('Issues are deleted!')
        else:
            print('There are no issues to be deleted!')

        return None

    @staticmethod
    def parse_issue(issue, field_mapping:dict):

        raw = issue.raw

        clean_alert_dict = {}
        clean_alert_dict['key'] = raw['key']

        for key, value in clean_alert_dict.items():
            clean_alert_dict[key] = value

        return clean_alert_dict

    @staticmethod
    def parse_issues(self, issues):

        clean_issue_list = []

        for issue in issues:
            clean_issue_list += [self.parse_issue(issue)]


        issues_df = pd.DataFrame(clean_issue_list)

        return issues_df

