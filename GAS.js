const FOLDER_ID = "your-folder-id-here"; // Replace with your Google Drive folder ID
const WEBHOOK_URL = "your-webhook-url-here"; // Replace with your webhook URL

function checkNewFilesInDrive() {
  const scriptProperties = PropertiesService.getScriptProperties();
  const lastCheck = scriptProperties.getProperty("lastCheck");
  const lastCheckTime = lastCheck ? new Date(lastCheck) : new Date(0);
  const newFiles = [];

  try {
    const folder = DriveApp.getFolderById(FOLDER_ID);
    const files = folder.getFiles();

    while (files.hasNext()) {
      try {
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
      } catch (fileErr) {
        console.error("❌ ファイル処理中にエラー:", fileErr);
      }
    }

    if (newFiles.length > 0) {
      try {
        const response = UrlFetchApp.fetch(WEBHOOK_URL, {
          method: "post",
          contentType: "application/json",
          payload: JSON.stringify({ files: newFiles }),
          muteHttpExceptions: true, // HTTPエラーでも例外を投げない（ログは出る）
        });

        if (response.getResponseCode() >= 400) {
          console.error(`❌ Webhook送信エラー: ${response.getResponseCode()} - ${response.getContentText()}`);
        } else {
          console.log("✅ Webhook送信成功");
        }
        scriptProperties.setProperty("lastCheck", new Date().toISOString());
      } catch (httpErr) {
        console.error("❌ Webhook送信中に例外が発生:", httpErr);
      }
    }


  } catch (err) {
    console.error("❌ 全体処理中にエラー:", err);
  }
}