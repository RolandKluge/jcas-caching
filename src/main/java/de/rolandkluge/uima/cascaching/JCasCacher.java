package de.rolandkluge.uima.cascaching;

import static org.apache.uima.fit.factory.AnalysisEngineFactory.createEngineDescription;
import static org.apache.uima.fit.factory.CollectionReaderFactory.createReaderDescription;

import java.io.File;
import java.io.FilenameFilter;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.RegexFileFilter;
import org.apache.uima.UIMAFramework;
import org.apache.uima.analysis_engine.AnalysisEngineDescription;
import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.apache.uima.collection.CollectionReaderDescription;
import org.apache.uima.fit.component.JCasAnnotator_ImplBase;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.util.Level;

import de.tudarmstadt.ukp.dkpro.core.io.xmi.XmiReader;
import de.tudarmstadt.ukp.dkpro.core.io.xmi.XmiWriter;

/**
 * This class caches JCas objects in XMI format.
 * 
 * <p>
 * The typical usage scenario looks as follows:
 * <ol>
 * <li>Define your reader for the data: CollectionReaderDescription reader = ...</li>
 * <li>Define the preprocessing whose results should also be cached: AnalysisEngineDescriptiont preprocessingEngine = ...</li>
 * <li>Instantiate the cacher: JCasCacher cacher = new JCasCacher(originalReader, preprocessingEngine)</li>
 * <li>Fetch the caching reader: CollectionReaderDescription cachingReader = cacher.getCachingReader()</li>
 * <li>Fetch the caching preprocessing: AnalysisEngineDescription cachingPreprocessing = cacher.getCachingPreprocessing()</li>
 * <li>Plug the caching components into the pipeline: SimplePipeline.runPipeline(cachingReader, cachingPreprocessing)</li>
 * </ol>
 * </p>
 * @author Roland Kluge
 *
 */
public class JCasCacher
{

    public static class DummyAnnotator
        extends JCasAnnotator_ImplBase
    {

        @Override
        public void process(final JCas aJCas)
            throws AnalysisEngineProcessException
        {
            // do nothing
        }

    }

    private CollectionReaderDescription originalReader;
    private AnalysisEngineDescription originalPreprocessingEngine;
    private boolean isUseCache;
    private File cacheDirectory;

    public JCasCacher(final CollectionReaderDescription originalReader,
            final AnalysisEngineDescription preprocessingEngine)
    {
        this.isUseCache = true;
        this.cacheDirectory = new File("./target/cascaching-cache");

        this.setOriginalReader(originalReader);
        this.setOriginalPreprocessing(preprocessingEngine);
    }

    public void setOriginalReader(final CollectionReaderDescription originalReader)
    {
        this.originalReader = originalReader;
    }

    public void setOriginalPreprocessing(final AnalysisEngineDescription preprocessingEngine)
    {
        this.originalPreprocessingEngine = preprocessingEngine;
    }

    public void setUseCache(final boolean enabled)
    {
        this.isUseCache = enabled;
    }

    public CollectionReaderDescription getCachingReader()
        throws ResourceInitializationException
    {
        final CollectionReaderDescription reader;
        this.checkDirectoryIsNotPolluted();

        if (this.isUseCache && hasCachedCASes()) {
            reader = createReaderDescription(XmiReader.class, //
                    XmiReader.PARAM_SOURCE_LOCATION, cacheDirectory, //
                    XmiReader.PARAM_PATTERNS, "[+]*.xmi");
        }
        else {
            UIMAFramework.getLogger().log(Level.INFO,
                    "Could not find cached CASes or caching disabled. Using original reader.");
            reader = this.originalReader;
        }
        return reader;
    }

    public AnalysisEngineDescription getCachingPreprocessing()
        throws ResourceInitializationException
    {
        final AnalysisEngineDescription preprocessing;

        this.checkDirectoryIsNotPolluted();

        if (this.isUseCache && hasCachedCASes()) {
            preprocessing = createEngineDescription(DummyAnnotator.class);
        }
        else {
            final AnalysisEngineDescription xmiWriter = createEngineDescription(XmiWriter.class,
                    XmiWriter.PARAM_TARGET_LOCATION, cacheDirectory);
            preprocessing = createEngineDescription(this.originalPreprocessingEngine, xmiWriter);
        }
        return preprocessing;
    }

    private void checkDirectoryIsNotPolluted()
        throws ResourceInitializationException
    {
        this.cacheDirectory.mkdir();

        final int allFilesCount = this.cacheDirectory.listFiles().length;
        final int cachedFilesCount = this.cacheDirectory
                .listFiles((FilenameFilter) new RegexFileFilter(".*(xmi|xml)$")).length;
        if (allFilesCount > cachedFilesCount) {
            throw new ResourceInitializationException(new IllegalStateException(String.format(
                    "The cache directory contains [%d] file(s) without suffix 'xmi' or 'xml'",
                    allFilesCount - cachedFilesCount)));
        }
    }

    private boolean hasCachedCASes()
    {
        final boolean hasTypesystem = new File(cacheDirectory, "typesystem.xml").exists();
        final boolean hasXmiFiles = !FileUtils.listFiles(this.cacheDirectory,
                new String[] { "xmi" }, false).isEmpty();
        return hasTypesystem && hasXmiFiles;
    }
}
