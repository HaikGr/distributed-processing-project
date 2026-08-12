pipeline {
    agent any

    stages {

        stage('Environment') {
            steps {
                sh '''
                    echo "===== ENVIRONMENT ====="
                    python3 --version
                    git --version
                    pwd
                    ls -la
                '''
            }
        }

        stage('Install Dependencies') {
            steps {
                dir('python-module') {
                    sh '''
                        rm -rf .venv
                        python3 -m venv .venv

                        source .venv/bin/activate

                        python -m ensurepip --upgrade
                        python -m pip install --upgrade pip
                        
                        pip install pytest build twine
                        pip install -e .
                    '''
                }
            }
        }

        stage('Test') {
            steps {
                dir('python-module') {
                    sh '''
                        source .venv/bin/activate
                        pytest
                    '''
                }
            }
        }

        stage('Build') {
            steps {
                dir('python-module') {
                    sh '''
                        source .venv/bin/activate

                        rm -rf dist/
                        python -m build

                        echo "===== BUILD ARTIFACTS ====="
                        ls -lah dist/
                    '''
                }
            }
        }

        stage('Publish to Nexus') {
            steps {
                dir('python-module') {
                    withCredentials([
                        usernamePassword(
                            credentialsId: 'nexus-key',
                            usernameVariable: 'NEXUS_USERNAME',
                            passwordVariable: 'NEXUS_PASSWORD'
                        )
                    ]) {
                        sh '''
                            source .venv/bin/activate

                            python -m twine upload \
                                --repository-url http://localhost:8081/repository/pypi-hosted/ \
                                --username "$NEXUS_USERNAME" \
                                --password "$NEXUS_PASSWORD" \
                                dist/*
                        '''
                    }
                }
            }
        }

    }
}