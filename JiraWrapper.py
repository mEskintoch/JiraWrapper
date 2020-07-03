class JiraFacilitator:

    def __init__(self, rsa_key, access_token, access_secret, jira_server, consumer_key):

        self.rsa_key = rsa_key
        self.access_token = access_token
        self.access_secret = access_secret
        self.JIRA_SERVER = jira_server
        self.CONSUMER_KEY = consumer_key
        self.jira_instance = self.connect()

    #         self.project = project
    #         self.project = project_key

    def all_issues(self, project):

        """
        Fetches all the issues from given jira project.

        Args:
          :param project: Jira Project (str)

        Returns:
          List of issues.
        """

        return self.jira_instance.search_issues('project = "{}"'.format(project), maxResults=False)

    def connect(self):

        """
        Connects to Jira server.

        Returns:
          jira object.
        """

        # Now you can use the access tokens with the JIRA client.
        jira = JIRA(options={'server': self.JIRA_SERVER}, oauth={
            'access_token': self.access_token,
            'access_token_secret': self.access_secret,
            'consumer_key': self.CONSUMER_KEY,
            'key_cert': self.rsa_key})

        return jira

    def post_issues(self, project, project_key, pdf, **kwargs):

        """
        Posts issues from a pandas dataframe to given jira project.

        Args:
          :param project: Jira Project (str)
          :param project_key: Jira Project Key (str)
          :param pdf: Pandas Dataframe
          :param kwargs: field map dictionary.
            Example:
                  kwargs = {"col" :
                            {
                              'issue_field' : 'column name'
                            },
                            "lit" :
                            {
                              'issue_field' : 'string'
                            }
                           }

        Returns:
          None
        """

        total = len(pdf)

        for index, row in pdf.iterrows():

            post_dict_ = {"project": {"name": project, "key": project_key}}

            if kwargs['lit']:
                for k, v in kwargs['lit'].items():
                    post_dict_[k] = v
            if kwargs['col']:
                for k, v in kwargs['col'].items():
                    post_dict_[k] = row[v]

            response = self.jira_instance.create_issue(post_dict_)
            print('Alerts posted {} / {}. : {}'.format(index + 1, total, response))

        return None

    def move_issues(self, pdf, issue_key_column, move_to: str, fields_map):

        """
        Function that executes issue transition within workflow. Takes input from
          pandas dataframe in the same way that is applied in self.post_issues.

        Args:
          :param pdf: Pandas Dataframe
          :param issue_key_column: Column name that contains issue key info.
          :param move_to: workflow name to make transition to.
          :param field_map: field map dictionary that contains issue info.
            Example:
                  kwargs = {"col" :
                            {
                              'issue_field' : 'column name'
                            },
                            "lit" :
                            {
                              'issue_field' : 'string'
                            }
                           }

        Returns:
          None
        """

        assert issue_key_column in pdf.columns, f"Column {issue_key_column} is not in the dataframe!"

        for index, row in pdf.iterrows():
            fields = {}

            if fields_map['lit']:
                for k, v in fields_map['lit'].items():
                    fields[k] = v
            if fields_map['col']:
                for k, v in fields_map['col'].items():
                    fields[k] = row[v]

            response = self.jira_instance.transition_issue(row[issue_key_column], move_to, fields)
            print('Ticket {} is moved to {}'.format(row[issue_key_column], move_to))

        return None

    def delete_issue(self, issue_key, delete_subtasks: bool = True):

        """
        Deletes a single issue with given issue key.

        Args:
          :param issue_key: Issue Key (str)

        Returns:
          None
        """

        return self.jira_instance.issue(issue_key).delete(deleteSubtasks=delete_subtasks)

    def delete_all_issues(self, project: str):

        """
        Deletes all the issues in a project.

        Args:
          :param project: jira project (str)

        Returns:
          None
        """

        print('Deleting all issues from project {}'.format(project))
        for issue in self.all_issues(project):
            self.delete_issue(issue, delete_subtasks=True)

    def jira_query(self, project: str, parse_issue_func):

        """
        Get ticket information from given jira project (project) in the
          given context (parse_issue_func)

        Args:
          :param project: Jira Project (str)
          :param parse_issue_funct: Python Function that applies on issue

        Returns:
          Spark Dataframe
        """

        # get all issues
        issues = self.all_issues(project=project)
        if len(issues) == 0:
            return None
        issue_list = [parse_issue_func(issue) for issue in issues]
        return spark.createDataFrame(Row(**d) for d in issue_list)

