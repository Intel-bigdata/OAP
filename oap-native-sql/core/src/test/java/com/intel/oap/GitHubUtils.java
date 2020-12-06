/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.intel.oap;

import io.jsonwebtoken.JwtBuilder;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import org.apache.commons.io.FileUtils;

import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;
import java.io.File;
import java.security.*;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.Base64;
import java.util.Date;
import java.util.Objects;

// code obtained from https://github-api.kohsuke.org/githubappjwtauth.html
public class GitHubUtils {

  public static PrivateKey get(String filename, String password) throws Exception {
    String base64Key = FileUtils.readFileToString(new File(filename));
    byte[] decoded = Base64.getMimeDecoder().decode(base64Key);
    MessageDigest md = MessageDigest.getInstance("MD5");
    Key aesKey = new SecretKeySpec(md.digest(password.getBytes()), "AES");
    Cipher cipher = Cipher.getInstance("AES");
    cipher.init(Cipher.DECRYPT_MODE, aesKey);
    byte[] decrypted = cipher.doFinal(decoded);
    PKCS8EncodedKeySpec spec = new PKCS8EncodedKeySpec(decrypted);
    KeyFactory kf = KeyFactory.getInstance("RSA");
    return kf.generatePrivate(spec);
  }

  public static String createJWT(String githubAppId, long ttlMillis, String password) throws Exception {
    //The JWT signature algorithm we will be using to sign the token
    SignatureAlgorithm signatureAlgorithm = SignatureAlgorithm.RS256;

    long nowMillis = System.currentTimeMillis();
    Date now = new Date(nowMillis);

    //We will sign our JWT with our private key
    Key signingKey = get(
        Objects.requireNonNull(GitHubUtils.class.getClassLoader().getResource(""))
            .getPath().concat(File.separator)
            .concat("tpch-ram-usage-reporter.2020-12-04.private-key.b64"), password);

    //Let's set the JWT Claims
    JwtBuilder builder = Jwts.builder()
        .setIssuedAt(now)
        .setIssuer(githubAppId)
        .signWith(signingKey, signatureAlgorithm);

    //if it has been specified, let's add the expiration
    if (ttlMillis > 0) {
      long expMillis = nowMillis + ttlMillis;
      Date exp = new Date(expMillis);
      builder.setExpiration(exp);
    }

    //Builds the JWT and serializes it to a compact, URL-safe string
    return builder.compact();
  }

  public static void main(String[] args) throws Exception {
    // encryption code
    String key = "{SET PASSWORD HERE}";
    String file = Objects.requireNonNull(GitHubUtils.class.getClassLoader().getResource(""))
        .getPath().concat(File.separator)
        .concat("tpch-ram-usage-reporter.2020-12-04.private-key.der");
    MessageDigest md = MessageDigest.getInstance("MD5");
    Key aesKey = new SecretKeySpec(md.digest(key.getBytes()), "AES");
    Cipher cipher = Cipher.getInstance("AES");
    cipher.init(Cipher.ENCRYPT_MODE, aesKey);
    byte[] encrypted = cipher.doFinal(FileUtils.readFileToByteArray(new File(file)));
    String s = Base64.getMimeEncoder().encodeToString(encrypted);
    FileUtils.write(new File("tpch-ram-usage-reporter.2020-12-04.private-key.b64"), s);
  }
}
