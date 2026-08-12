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
                        python3 -m venv .venv
                        . .venv/bin/activate

                        python -m pip install --upgrade pip
                        pip install build pytest twine
                    '''
                }
            }
        }

        stage('Test') {
            steps {
                dir('python-module') {
                    sh '''
                        . .venv/bin/activate
                        pytest
                    '''
                }
            }
        }

        stage('Build') {
            steps {
                dir('python-module') {
                    sh '''
                        . .venv/bin/activate

                        rm -rf dist/
                        python -m build

                        echo "===== BUILD ARTIFACTS ====="
                        ls -lah dist/
                    '''
                }
            }
        }

    }
}