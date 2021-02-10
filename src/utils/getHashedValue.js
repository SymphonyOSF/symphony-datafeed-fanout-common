import crypto from 'crypto';

const getHashedValue = (value) => {
    return crypto.createHash('md5').update(`${value}`).digest('hex');
};

export default getHashedValue;
