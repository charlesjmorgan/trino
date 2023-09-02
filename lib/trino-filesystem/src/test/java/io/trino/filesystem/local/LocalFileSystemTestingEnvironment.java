/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.filesystem.local;

import io.trino.filesystem.AbstractTrinoFileSystemTestingEnvironment;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.Iterator;
import java.util.stream.Stream;

import static java.util.function.Predicate.not;
import static org.assertj.core.api.Assertions.assertThat;

public class LocalFileSystemTestingEnvironment
        extends AbstractTrinoFileSystemTestingEnvironment
{
    private final Path tempDirectory;
    private final LocalFileSystem fileSystem;

    public LocalFileSystemTestingEnvironment()
            throws IOException
    {
        tempDirectory = Files.createTempDirectory("test");
        fileSystem = new LocalFileSystem(tempDirectory);
    }

    public void cleanupFiles()
            throws IOException
    {
        // tests will leave directories
        try (Stream<Path> walk = Files.walk(tempDirectory)) {
            Iterator<Path> iterator = walk.sorted(Comparator.reverseOrder()).iterator();
            while (iterator.hasNext()) {
                Path path = iterator.next();
                if (!path.equals(tempDirectory)) {
                    Files.delete(path);
                }
            }
        }
    }

    public void close()
            throws IOException
    {
        Files.delete(tempDirectory);
    }

    @Override
    protected boolean isHierarchical()
    {
        return true;
    }

    @Override
    public TrinoFileSystem getFileSystem()
    {
        return fileSystem;
    }

    @Override
    protected Location getRootLocation()
    {
        return Location.of("local://");
    }

    @Override
    protected void verifyFileSystemIsEmpty()
    {
        try {
            try (Stream<Path> entries = Files.list(tempDirectory)) {
                assertThat(entries.filter(not(tempDirectory::equals)).findFirst()).isEmpty();
            }
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
