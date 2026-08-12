pipeline {
    agent any

    stages {

        stage('Environment') {
            steps {
                sh '''
                    echo "===== SYSTEM ====="
                    whoami
                    pwd

                    echo "===== PYTHON ====="
                    python3 --version
                    which python3

                    echo "===== GIT ====="
                    git --version

                    echo "===== PROJECT ====="
                    ls -la
                '''
            }
        }

    }
}
