const path = require("path");

const rootDir = __dirname;
const logDir = path.join(rootDir, "logs", "pm2");

module.exports = {
  apps: [
    {
      name: "openlianghua-api",
      cwd: rootDir,
      script: path.join(rootDir, "scripts", "start_research_api.sh"),
      interpreter: "bash",
      autorestart: true,
      watch: false,
      time: true,
      env: {
        API_PORT: "8989",
      },
      out_file: path.join(logDir, "research_api.out.log"),
      error_file: path.join(logDir, "research_api.err.log"),
    },
    {
      name: "openlianghua-web",
      cwd: rootDir,
      script: path.join(rootDir, "scripts", "start_react_web.sh"),
      interpreter: "bash",
      autorestart: true,
      watch: false,
      time: true,
      env: {
        REACT_WEB_PORT: "5174",
      },
      out_file: path.join(logDir, "react_web.out.log"),
      error_file: path.join(logDir, "react_web.err.log"),
    },
  ],
};
