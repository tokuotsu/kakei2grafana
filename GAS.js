const FOLDER_ID = "your-folder-id-here"; // Replace with your Google Drive folder ID
const WEBHOOK_URL = "your-webhook-url-here"; // Replace with your webhook URL

function checkNewFilesInDrive() {
  const folder = DriveApp.getFolderById(FOLDER_ID);
  const files = folder.getFiles();

  const scriptProperties = PropertiesService.getScriptProperties();
  const lastCheck = scriptProperties.getProperty("lastCheck");
  const lastCheckTime = lastCheck ? new Date(lastCheck) : new Date(0);

  const newFiles = [];

  while (files.hasNext()) {
    const file = files.next();
    const createdTime = file.getDateCreated();

    if (createdTime > lastCheckTime) {
      file.setSharing(DriveApp.Access.ANYONE_WITH_LINK, DriveApp.Permission.VIEW);
      newFiles.push({
        name: file.getName(),
        url: file.getUrl(),
        id: file.getId(),
      });
    }
  }

  if (newFiles.length > 0) {
    UrlFetchApp.fetch(WEBHOOK_URL, {
      method: "post",
      contentType: "application/json",
      payload: JSON.stringify({ files: newFiles }),
    });
  }

  scriptProperties.setProperty("lastCheck", new Date().toISOString());
}
