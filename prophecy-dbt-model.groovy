pipeline {
    agent any

    environment {
        // Credentials IDs configured in Jenkins Credentials Store
        GIT_CREDENTIALS_ID = 'your-git-credentials'
        GCP_CREDENTIALS_ID = 'your-gcp-service_account_key'

        // dbt profiles directory
        DBT_PROFILES_DIR = "${WORKSPACE}/profiles"

        // BigQuery project details
        DBT_BIGQUERY_PROJECT = "your-bigquery-project"
        DBT_BIGQUERY_DATASET = "your-dataset-name"

        // Prophecy repository directory
        PROPHECY_DIR = "${WORKSPACE}/your-prophecy-dbt-repo"

        // PATH
        PATH = "/usr/local/bin:${PATH}" 
    }

    stages {
        stage('Checkout') {
            steps {
                checkout([
                    $class: 'GitSCM',
                    branches: [[name: 'main']],
                    userRemoteConfigs: [
                        [url: 'git@github.com:your-repo/your-prophecy-dbt-repo.git',
                         credentialsId: GIT_CREDENTIALS_ID]
                    ]
                ])
            }
        }

        stage('Set up dbt Environment') {
            steps {
                sh '''
                    # create profiles directory
                    mkdir -p $DBT_PROFILES_DIR

                    # create profiles.yml with BigQuery credentials
                    cat > $DBT_PROFILES_DIR/profiles.yml <<EOL
                    default:
                      target: bigquery
                      outputs:
                        bigquery:
                          type: bigquery
                          method: service_account_json_keyfile
                          project: $DBT_BIGQUERY_PROJECT
                          dataset: $DBT_BIGQUERY_DATASET
                          keyfile: $HOME/gcp_keyfile.json
                    EOL
                '''
                // Store GCP credentials
                withCredentials([file(credentialsId: GCP_CREDENTIALS_ID, variable:'GCP_KEYFILE')]) {
                    sh '''
                        cp "$GCP_KEYFILE" "$HOME/gcp_keyfile.json"
                       chmod 600 "$HOME/gcp_keyfile.json"
                    '''
                }
            }
        }

        stage('dbt Compile') {
            steps {
                sh '''
                    cd $PROPHECY_DIR
                    dbt compile
                '''
            }
        }

        stage('dbt Test') {
            steps {
                sh '''
                    cd $PROPHECY_DIR
                    dbt test
                '''
            }
        }

        stage('dbt Run') {
            steps {
                sh '''
                    cd $PROPHECY_DIR
                    dbt run
                '''
            }
        }
    }

    post {
        success {
            echo "✅ dbt + Prophecy pipeline completed successfully!"
        }
        failure {
            echo "❌ dbt + Prophecy pipeline failed!"
        }
    }
}
