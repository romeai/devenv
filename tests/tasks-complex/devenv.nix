{
  tasks = {
    "frontend:build" = {
      exec = "echo 'Building frontend...'";
      after = [ "frontend:test" ];
      cache.inputs = [{ path = "src/frontend/*.js"; optional = true; } { path = "src/frontend/*.css"; optional = true; }];
    };

    "frontend:test" = {
      exec = "echo 'Testing frontend...'";
      after = [ "frontend:lint" ];
      status = "test -f .frontend-test-passed";
    };

    "frontend:lint" = {
      exec = "echo 'Linting frontend...'";
    };

    "backend:build" = {
      exec = "echo 'Building backend...'";
      after = [ "backend:test" ];
      cache.inputs = [{ path = "src/backend/**/*.py"; optional = true; }];
    };

    "backend:test" = {
      exec = "echo 'Testing backend...'";
      after = [ "backend:lint" ];
    };

    "backend:lint" = {
      exec = "echo 'Linting backend...'";
      status = "which ruff";
    };

    "deploy:production" = {
      exec = "echo 'Deploying to production...'";
      after = [ "frontend:build" "backend:build" ];
    };

    "docs:generate" = {
      exec = "echo 'Generating documentation...'";
      cache.inputs = [{ path = "docs/**/*.md"; optional = true; }];
    };

    "docs:publish" = {
      exec = "echo 'Publishing documentation...'";
      after = [ "docs:generate" ];
    };
  };
}
