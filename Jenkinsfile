pipeline {
    agent any

    triggers {
        githubPush()
    }

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
            when {
                changeset "python-module/**"
            }

            steps {
                dir('python-module') {
                    sh '''
                        rm -rf .venv
                        python3 -m venv .venv

                        . .venv/bin/activate

                        python -m ensurepip --upgrade
                        python -m pip install --upgrade pip

                        pip install pytest build twine
                    '''
                }
            }
        }

        stage('Test') {
            when {
                changeset "python-module/**"
            }

            steps {
                dir('python-module') {
                    sh '''
                        . .venv/bin/activate

                        pip install -e .
                        pytest
                    '''
                }
            }
        }

        stage('Version') {
            when {
                changeset "python-module/**"
            }

            steps {
                dir('python-module') {
                    sh '''
                        . .venv/bin/activate

                        python scripts/bump_version.py

                        echo "===== VERSION ====="
                        grep '^version' pyproject.toml
                    '''
                }
            }
        }

        stage('Build') {
            when {
                changeset "python-module/**"
            }

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

        stage('Publish to Nexus') {
            when {
                changeset "python-module/**"
            }

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
                            . .venv/bin/activate

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

        stage('Tag Release') {
            when {
                changeset "python-module/**"
            }

            steps {
                dir('python-module') {
                    sh '''
                        VERSION=$(grep '^version' pyproject.toml | sed -E 's/.*"([^"]+)".*/\\1/')

                        echo "===== TAGGING v${VERSION} ====="

                        git config user.name "HaikGr"
                        git config user.email "haykgrgiroyanpersonal@gmail.com"

                        git tag "v${VERSION}"
                        git push origin "v${VERSION}"
                    '''
                }
            }
        }
    }
}