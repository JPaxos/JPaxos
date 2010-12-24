package lsr.common;

import java.io.File;

public class DirectoryHelper {
    public static void create(String path) {
        File file = new File(path);
        if (file.exists()) {
            delete(file.getAbsolutePath());
        }

        file.mkdirs();
    }

    public static void delete(String path) {
        if (!deleteDir(new File(path))) {
            throw new RuntimeException("Directory was not removed");
        }
    }

    private static boolean deleteDir(File dir) {
        if (dir.isDirectory()) {
            String[] children = dir.list();
            for (int i = 0; i < children.length; i++) {
                boolean success = deleteDir(new File(dir, children[i]));
                if (!success) {
                    return false;
                }
            }
        }

        // The directory is now empty so delete it
        return dir.delete();
    }
}
