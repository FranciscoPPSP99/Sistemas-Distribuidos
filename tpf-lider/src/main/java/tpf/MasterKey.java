package tpf;

import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.attribute.AclEntry;
import java.nio.file.attribute.AclEntryPermission;
import java.nio.file.attribute.AclEntryType;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.AclFileAttributeView;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.SecureRandom;
import java.util.List;

final class MasterKey {
    private static final int SIZE = 32;

    private MasterKey() {
    }

    static byte[] loadOrCreate(Path path) throws IOException {
        Path target = path.toAbsolutePath();
        try {
            return read(target);
        } catch (NoSuchFileException missing) {
            return create(target);
        }
    }

    private static byte[] read(Path path) throws IOException {
        try (var input = Files.newInputStream(path, LinkOption.NOFOLLOW_LINKS)) {
            byte[] key = input.readNBytes(SIZE + 1);
            if (key.length != SIZE) {
                throw new IOException("A master.key tem de conter exatamente 32 bytes.");
            }
            return key;
        }
    }

    private static byte[] create(Path target) throws IOException {
        Path parent = target.getParent();
        // Escreve num ficheiro privado antes de disponibilizar a chave com o nome final.
        Path temporary = Files.createTempFile(parent, ".master-key-", ".tmp", ownerPermissions(parent));
        try {
            restrictInheritedAccess(temporary);
            byte[] key = new byte[SIZE];
            new SecureRandom().nextBytes(key);
            Files.write(temporary, key);
            try {
                Files.createLink(target, temporary);
            } catch (FileAlreadyExistsException alreadyCreated) {
                return read(target);
            }
            return key;
        } finally {
            Files.deleteIfExists(temporary);
        }
    }

    private static void restrictInheritedAccess(Path path) throws IOException {
        if (Files.getFileStore(path).supportsFileAttributeView("posix")) {
            return;
        }
        var view = Files.getFileAttributeView(path, AclFileAttributeView.class);
        var owner = view.getOwner();
        if (view.getAcl().stream().anyMatch(entry ->
                entry.type() == AclEntryType.ALLOW && !entry.principal().equals(owner))) {
            throw new IOException("A pasta herda permissões de terceiros. Usa uma pasta privada para a master.key.");
        }
    }

    private static FileAttribute<?> ownerPermissions(Path directory) throws IOException {
        var store = Files.getFileStore(directory);
        if (store.supportsFileAttributeView("posix")) {
            return PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-------"));
        }
        if (store.supportsFileAttributeView("acl")) {
            var owner = directory.getFileSystem().getUserPrincipalLookupService()
                    .lookupPrincipalByName(System.getProperty("user.name"));
            AclEntry entry = AclEntry.newBuilder()
                    .setType(AclEntryType.ALLOW)
                    .setPrincipal(owner)
                    .setPermissions(AclEntryPermission.values())
                    .build();
            return new FileAttribute<List<AclEntry>>() {
                @Override
                public String name() {
                    return "acl:acl";
                }

                @Override
                public List<AclEntry> value() {
                    return List.of(entry);
                }
            };
        }
        throw new IOException("O sistema de ficheiros não permite restringir o acesso à master.key.");
    }
}
