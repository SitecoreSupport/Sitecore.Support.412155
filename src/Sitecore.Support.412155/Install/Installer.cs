using System;
using System.Collections.Specialized;
using System.IO;
using Sitecore.Globalization;
using Sitecore.Install.Files;
using Sitecore.Install.Framework;
using Sitecore.Install.Items;
using Sitecore.Install.Metadata;
using Sitecore.Install.Security;
using Sitecore.Install.Zip;
using Sitecore.Install.Utils;
using Sitecore.Install.BlobData;
using Sitecore.IO;
using Sitecore.Web;
using Sitecore.Diagnostics;
using Sitecore.Data.Items;
using Sitecore.Data;
using Sitecore.Configuration;
using Sitecore.Reflection;

namespace Sitecore.Install
{
    using System.Collections.Generic;
    using Sitecore.Common;
    using Sitecore.Events;
    using Sitecore.Install.Events;

    /// <summary>
    /// Installer class
    /// </summary>
    public class Installer : MarshalByRefObject
    {
        /// <summary>
        /// Executes the post step.
        /// </summary>
        /// <param name="action">The action.</param>
        /// <param name="context">The context.</param>
        public void ExecutePostStep(string action, IProcessingContext context)
        {
            if (string.IsNullOrEmpty(action))
            {
                return;
            }
            try
            {
                var postStepInstallationArgs = new InstallationEventArgs(new List<ItemUri>(), new List<FileCopyInfo>(), "packageinstall:poststep:starting");
                Event.RaiseEvent("packageinstall:poststep:starting", postStepInstallationArgs);

                action = action.Trim();

                if (action.StartsWith("/", StringComparison.InvariantCulture))
                {
                    action = Globals.ServerUrl + action;
                }

                if (action.IndexOf("://", StringComparison.InvariantCulture) > -1)
                {
                    //execute web request
                    try
                    {
                        WebUtil.ExecuteWebPage(action);
                    }
                    catch (Exception ex)
                    {
                        Log.Error("Error executing post step for package", ex, this);
                    }
                }
                else
                {
                    object instance = null;
                    try
                    {
                        instance = ReflectionUtil.CreateObject(action);
                    }
                    catch
                    {
                    }
                    if (instance != null)
                    {
                        if (instance is IPostStep)
                        {
                            ITaskOutput output = context.Output;
                            NameValueCollection metadata = new MetadataView(context).Metadata;
                            (instance as IPostStep).Run(output, metadata);
                        }
                        else
                        {
                            ReflectionUtil.CallMethod(instance, "RunPostStep");
                        }
                    }
                    else
                    {
                        Log.Error(string.Format("Execution of post step failed: Class '{0}' wasn't found.", action), this);
                    }
                }
            }
            finally
            {
                var postStepInstallationArgs = new InstallationEventArgs(new List<ItemUri>(), new List<FileCopyInfo>(), "packageinstall:poststep:ended");
                Event.RaiseEvent("packageinstall:poststep:ended", postStepInstallationArgs);
                var installationArgs = new InstallationEventArgs(new List<ItemUri>(), new List<FileCopyInfo>(), "packageinstall:ended");
                Event.RaiseEvent("packageinstall:ended", installationArgs);
            }
        }

        /// <summary>
        /// Gets the filename.
        /// </summary>
        /// <param name="filename">The filename.</param>
        /// <returns>The filename.</returns>
        public static string GetFilename(string filename)
        {
            Error.AssertString(filename, "filename", true);

            string result = filename;

            if (!FileUtil.IsFullyQualified(result))
            {
                result = FileUtil.MakePath(Settings.PackagePath, result);
            }

            return result;
        }

        /// <summary>
        /// Gets the post step.
        /// </summary>
        /// <param name="context">The context.</param>
        /// <returns>The post step.</returns>
        public static string GetPostStep(IProcessingContext context)
        {
            return StringUtil.GetString(new MetadataView(context).PostStep);
        }

        /// <summary>
        /// Installs the package.
        /// </summary>
        /// <param name="path">The path.</param>
        public void InstallPackage(string path)
        {
            InstallPackage(path, CreateInstallationContext());
        }

        /// <summary>
        /// Installs the package.
        /// </summary>
        /// <param name="path">The path.</param>
        /// <param name="registerInstallation">if set to <c>true</c> the package installation will be registered.</param>
        public void InstallPackage(string path, bool registerInstallation)
        {
            InstallPackage(path, registerInstallation, CreateInstallationContext());
        }

        /// <summary>
        /// Installs the package.
        /// </summary>
        /// <param name="path">The path.</param>
        /// <param name="source">The source.</param>
        public void InstallPackage(string path, ISource<PackageEntry> source)
        {
            InstallPackage(path, source, CreateInstallationContext());
        }

        /// <summary>
        /// Installs the package.
        /// </summary>
        /// <param name="path">The path.</param>
        /// <param name="registerInstallation">if set to <c>true</c> the package installation will be registered.</param>
        /// <param name="source">The source.</param>
        public void InstallPackage(string path, bool registerInstallation, ISource<PackageEntry> source)
        {
            InstallPackage(path, registerInstallation, source, CreateInstallationContext());
        }

        /// <summary>
        /// Installs the package.
        /// </summary>
        /// <param name="path">The path.</param>
        /// <param name="context">The processing context.</param>
        public void InstallPackage(string path, IProcessingContext context)
        {
            ISource<PackageEntry> source = new PackageReader(path);
            InstallPackage(path, source, context);
        }

        /// <summary>
        /// Installs the package.
        /// </summary>
        /// <param name="path">The path.</param>
        /// <param name="registerInstallation">if set to <c>true</c> package installation will be registered.</param>
        /// <param name="context">The processing context.</param>
        public void InstallPackage(string path, bool registerInstallation, IProcessingContext context)
        {
            ISource<PackageEntry> source = new PackageReader(path);
            InstallPackage(path, registerInstallation, source, context);
        }

        /// <summary>
        /// Installs the package.
        /// </summary>
        /// <param name="path">The path.</param>
        /// <param name="source">The source.</param>
        /// <param name="context">The processing context.</param>
        public void InstallPackage(string path, ISource<PackageEntry> source, IProcessingContext context)
        {
            this.InstallPackage(path, true, source, context);
        }

        /// <summary>
        /// Installs the package.
        /// </summary>
        /// <param name="path">The path.</param>
        /// <param name="registerInstallation">if set to <c>true</c> [register installation].</param>
        /// <param name="source">The source.</param>
        /// <param name="context">The processing context.</param>
        public void InstallPackage(string path, bool registerInstallation, ISource<PackageEntry> source, IProcessingContext context)
        {
            var installationArgs = new InstallationEventArgs(null, null, "packageinstall:starting");
            Event.RaiseEvent("packageinstall:starting", installationArgs);

            Log.Info("Installing package: " + path, this);

            using (new PackageInstallationContext())
            {
                using (ConfigWatcher.PostponeEvents())
                {
                    ISink<PackageEntry> installer = CreateInstallerSink(context);
                    new EntrySorter(source).Populate(installer);

                    installer.Flush();
                    installer.Finish();

                    if (registerInstallation)
                    {
                        RegisterPackage(context);
                    }

                    foreach (IProcessor<IProcessingContext> processor in context.PostActions)
                    {
                        processor.Process(context, context);
                    }
                }
            }
        }

        /// <summary>
        /// Installs the security accounts from the package.
        /// </summary>
        /// <param name="path">The path to the package file.</param>
        public void InstallSecurity([NotNull] string path)
        {
            Assert.ArgumentNotNullOrEmpty(path, "path");

            InstallSecurity(path, new SimpleProcessingContext());
        }

        /// <summary>
        /// Installs the security accounts from the package.
        /// </summary>
        /// <param name="path">The path to the package file.</param>
        /// <param name="context">The context.</param>
        public void InstallSecurity([NotNull] string path, [NotNull] IProcessingContext context)
        {
            Assert.ArgumentNotNullOrEmpty(path, "path");
            Assert.ArgumentNotNull(context, "context");

            Log.Info("Installing security from package: " + path, this);
            var reader = new PackageReader(path);
            var installer = new AccountInstaller();
            installer.Initialize(context);
            reader.Populate(installer);
            installer.Flush();
            installer.Finish();
        }

        /// <summary>
        /// Creates an installer sink.
        /// </summary>
        /// <returns>An installer sink.</returns>
        public static ISink<PackageEntry> CreateInstallerSink(IProcessingContext context)
        {
            SinkDispatcher dispatcher = new SinkDispatcher(context);
            dispatcher.AddSink(Constants.MetadataPrefix, new MetadataSink(context));
            dispatcher.AddSink(Constants.BlobDataPrefix, new BlobInstaller(context));
            dispatcher.AddSink(Constants.ItemsPrefix, new LegacyItemUnpacker(new ItemInstaller(context)));
            dispatcher.AddSink(Constants.FilesPrefix, new FileInstaller(context));
            return dispatcher;
        }

        /// <summary>
        /// Creates the installation context.
        /// </summary>
        /// <returns>The installation context.</returns>
        public static IProcessingContext CreateInstallationContext()
        {
            return new SimpleProcessingContext();
        }

        /// <summary>
        /// Creates the preview context.
        /// </summary>
        /// <returns>The preview context.</returns>
        public static IProcessingContext CreatePreviewContext()
        {
            SimpleProcessingContext context = new SimpleProcessingContext();
            context.SkipData = true;
            context.SkipErrors = true;
            context.SkipCompression = true;
            context.ShowSourceInfo = true;
            return context;
        }

        #region private scope

        /// <summary>
        /// Creates an item with information about package installation.
        /// </summary>
        /// <param name="context">The context.</param>
        protected virtual void RegisterPackage([NotNull] IProcessingContext context)
        {
            Assert.ArgumentNotNull(context, "context");

            MetadataView view = new MetadataView(context);
            string packageName = view.PackageName;
            bool packageNameValid;
            try
            {
                packageNameValid = ItemUtil.IsItemNameValid(packageName);
            }
            catch (Exception)
            {
                packageNameValid = false;
            }
            if (!packageNameValid && packageName.Length > 0)
            {
                packageName = ItemUtil.ProposeValidItemName(packageName);
            }
            if (packageName.Length == 0)
            {
                packageName = Translate.Text("Unnamed Package");
            }

            Item item = CreateRegistrationItem(packageName);
            if (item != null)
            {
                item.Editing.BeginEdit();
                item[PackageRegistrationFieldIDs.PackageName] = view.PackageName;
                item[PackageRegistrationFieldIDs.PackageID] = view.PackageID;
                item[PackageRegistrationFieldIDs.PackageVersion] = view.Version;
                item[PackageRegistrationFieldIDs.PackageAuthor] = view.Author;
                item[PackageRegistrationFieldIDs.PackagePublisher] = view.Publisher;
                item[PackageRegistrationFieldIDs.PackageReadme] = view.Readme;
                item[PackageRegistrationFieldIDs.PackageRevision] = view.Revision;
                item.Editing.EndEdit();
            }
            else
            {
                Log.Error("Could not get registration item for package: " + packageName, this);
            }
        }

        /// <summary>
        /// Creates the registration item.
        /// </summary>
        /// <param name="name">The name.</param>
        /// <returns></returns>
        [CanBeNull]
        private Item CreateRegistrationItem(string name)
        {
            Database database = Factory.GetDatabase("core");
            if (database != null)
            {
                TemplateItem node = database.Templates[TemplateIDs.Node];
                TemplateItem item = database.Templates[TemplateIDs.PackageRegistration];
                if ((node != null) && (item != null))
                {
                    string path = "/sitecore/system/Packages/Installation history/" + name + "/" + DateUtil.IsoNow;
                    return database.CreateItemPath(path, node, item);
                }
            }
            return null;
        }

        #endregion private scope

        /// <summary>
        /// Restarts the sitecore server.
        /// </summary>
        public static void RestartServer()
        {
            string configFilename = FileUtil.MapPath("/web.config");
            FileInfo info = new FileInfo(configFilename);
            info.LastWriteTimeUtc = DateTime.UtcNow;
        }
    }
}
