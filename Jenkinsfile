pipeline {
    agent any

    stages {

        stage('Environment') {
            steps {
                sh '''
                    python3 --version
                    git --version
                    pwd
                    ls -la
                '''
            }
        }

        stage('Install Dependencies') {
            steps {
                sh '''
                    python3 -m venv .venv

                    . .venv/bin/activate

                    python -m pip install --upgrade pip
                    pip install build pytest twine
                '''
            }
        }

        stage('Test') {
            steps {
                sh '''
                . .venv/bin/activate
                pytest
                '''
                }
            } 	

        stage('Build') {
            steps {
            sh '''
                . .venv/bin/activate

                rm -rf python-module/dist/
                python -m build

                echo "===== BUILD ARTIFACTS ====="
                ls -lah python-module/dist/
            '''
            }
        }
}
