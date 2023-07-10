package launcher.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.X509EncodedKeySpec;
import java.util.Base64;
import java.util.Random;

public class SecurityUtils {
    static Logger logger = LoggerFactory.getLogger(SecurityUtils.class);
    public static String encrypt(String publicKey, String jsonContent) throws NoSuchPaddingException, NoSuchAlgorithmException, InvalidKeyException, IllegalBlockSizeException, BadPaddingException, InvalidKeySpecException, JsonProcessingException {
        // Generate an AES key and encrypt the connection information...
        logger.info(String.format("publicKey: %s", publicKey));
        var random = new Random();
        var preKey = new byte[16];
        random.nextBytes(preKey);
        logger.info(String.format("aes_key: '%s'", (Object) preKey));
        var aesKey = new SecretKeySpec(preKey, "AES");
        var aesCipher = Cipher.getInstance("AES");
        aesCipher.init(Cipher.ENCRYPT_MODE, aesKey);
        var connInfo = Base64.getEncoder().encodeToString(aesCipher.doFinal(jsonContent.getBytes(StandardCharsets.UTF_8)));

        // Encrypt the AES key using the public key...
        var encodedPK = publicKey.getBytes(StandardCharsets.UTF_8);
        var b64Key = Base64.getDecoder().decode(encodedPK);
        var keySpec = new X509EncodedKeySpec(b64Key);
        var keyFactory = KeyFactory.getInstance("RSA");
        var rsaKey = keyFactory.generatePublic(keySpec);

        var rsaCipher = Cipher.getInstance("RSA");
        rsaCipher.init(Cipher.ENCRYPT_MODE, rsaKey);
        var key = Base64.getEncoder().encodeToString(rsaCipher.doFinal(aesKey.getEncoded()));
        return Base64.getEncoder().encodeToString(Payload.createJson(key, connInfo).getBytes(StandardCharsets.UTF_8));
    }
}
