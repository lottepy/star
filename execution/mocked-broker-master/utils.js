function getRandomInt(min, max) {
  min = Math.ceil(min);
  max = Math.floor(max);
  return Math.floor(Math.random() * (max - min + 1)) + min;
}

exports.getRandomOrderId = () => {
  return 'PM' + getRandomInt(100_000_000_000, 999_999_999_999);
}

exports.getRandomExecutionId = () => {
  return 'EXE' + getRandomInt(100_000_000_000, 999_999_999_999);
}

exports.getRandomInt = getRandomInt;
exports.getRandomNumber = function randomNumber(min, max) {
    return Math.random() * (max - min) + min;
}

const padDateZero = (v) => {
    return (v < 10) ? '0' + v : v
}

exports.getDateString = () => {
    let d = new Date();
    let year = d.getFullYear()
    let month = padDateZero(d.getMonth() + 1)
    let day = padDateZero(d.getDate())
    let hour = padDateZero(d.getHours())
    let min = padDateZero(d.getMinutes())
    let sec = padDateZero(d.getSeconds())
    //YYYYMMDDhhmmss
    return year + month + day + hour + min + sec
}

exports.getDateStringDashSeperated = () => {
    let d = new Date();
    let year = d.getFullYear()
    let month = padDateZero(d.getMonth() + 1)
    let day = padDateZero(d.getDate())
    let hour = padDateZero(d.getHours())
    let min = padDateZero(d.getMinutes())
    let sec = padDateZero(d.getSeconds())
    //YYYYMMDDhhmmss
    return year + '-' + month + '-' + day + '-' + hour + '-' + min + '-' + sec
}

exports.LOG_PREFIX = exports.getDateStringDashSeperated()

exports.logRequest = (req) => {
    const ip = req.headers['x-forwarded-for'] || req.connection.remoteAddress || req.socket.remoteAddress ||
        (req.connection.socket ? req.connection.socket.remoteAddress : "no_ip_found");
    console.log(exports.LOG_PREFIX, ': ', req.method, req.originalUrl, ip, req.params, req.query)
}

