var multer = require('multer');
const path = require('path');

class FileUploadService {
  constructor() {}

  fileUpload(savePath) {
    const fileStorage = multer.diskStorage({
      destination: function (req, file, cb) {
        cb(null, savePath);
      },
      filename: function (req, file, cb) {
        cb(null, Date.now() + path.extname(file.originalname));
      }
    });

    return multer({ storage: fileStorage });
  }
}

module.exports = FileUploadService;
