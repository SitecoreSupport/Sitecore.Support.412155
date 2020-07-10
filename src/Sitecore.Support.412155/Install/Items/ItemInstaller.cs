using System;
using System.Collections.Generic;
using System.Threading;
using System.Xml;
using Sitecore.Collections;
using Sitecore.Configuration;
using Sitecore.Data;
using Sitecore.Data.Items;
using Sitecore.Diagnostics;
using Sitecore.Globalization;
using Sitecore.Install.BlobData;
using Sitecore.Install.Framework;
using Sitecore.Install.Utils;
using Sitecore.Xml;

namespace Sitecore.Install.Items
{
    using System.Linq;
    using Sitecore.Events;
    using Sitecore.Install.Events;
    using Sitecore.Install.Files;

    #region VersionInstallMode enumeration

    /// <summary>
    /// Version install mode
    /// </summary>


    #endregion

    /// <summary>
    /// Item installer
    /// </summary>
    public class ItemInstaller : AdvancedBaseSink<PackageEntry, ItemInstallerContext>
    {
        #region Fields

        private static Func<ItemReference, XmlVersionParser, Item, DateTime, Item> createItem = (item, parser, parent, created) => Data.Managers.ItemManager.CreateItem(
            parser.Name,
            parent,
            parser.TemplateID,
            item.ID,
            created,
            SecurityModel.SecurityCheck.Disable);

        readonly SafeDictionary<string, List<ID>> _IDsAlreadyInstalled = new SafeDictionary<string, List<ID>>();
        readonly SafeDictionary<string, List<ID>> _IDsToBeInstalled = new SafeDictionary<string, List<ID>>();
        readonly List<PackageEntry> _installationQueue = new List<PackageEntry>();
        readonly IList<KeyValuePair<string, string>> _pendingDeleteItems = new List<KeyValuePair<string, string>>();

        /// <summary>
        /// Contains entries that should be removed from deletion queue.
        /// </summary>
        private readonly IList<KeyValuePair<string, string>> removeFromDeletionQueue = new List<KeyValuePair<string, string>>();

        /// <summary>
        /// Contains identifiers of created items.
        /// </summary>
        private readonly HashSet<ID> createdItems = new HashSet<ID>();

        /// <summary>
        /// Contains entries that have not installed template yet
        /// </summary>
        readonly List<PackageEntry> _postponedToInstall = new List<PackageEntry>();
        List<ItemUri> installedItems = new List<ItemUri>();
        #endregion

        #region Constructors

        /// <summary>
        /// Initializes a new instance of the <see cref="ItemInstaller"/> class.
        /// </summary>
        /// <param name="context">The context.</param>
        public ItemInstaller(IProcessingContext context)
        {
            base.Initialize(context);
        }

        #endregion

        #region Public methods

        /// <summary>
        /// Finishes the sink.
        /// </summary>
        public override void Finish()
        {
            ProcessingContext.PostActions.Add(new ContentRestorer(PendingDeleteItems));
        }

        /// <summary>
        /// Flushes the sink
        /// </summary>
        public override void Flush()
        {
            if (_installationQueue.Count == 0)
            {
                return;
            }


            try
            {
                while (true)
                {
                    _postponedToInstall.Clear();

                    int itemsToInstallCount = _installationQueue.Count;


                    foreach (PackageEntry entry in _installationQueue)
                    {

                        InstallEntry(entry);
                    }

                    var args = new InstallationEventArgs(installedItems, new List<FileCopyInfo>(), "packageinstall:items:starting");
                    Event.RaiseEvent("packageinstall:items:starting", args);

                    _installationQueue.Clear();

                    // If all items were installed - exit
                    if (_postponedToInstall.Count == 0)
                    {
                        return;
                    }

                    // If no items were installed - throw an exception
                    if (_postponedToInstall.Count == itemsToInstallCount)
                    {
                        throw new Exception("Cannot install templates structure. There're some cyclic references or some template is under an item having been created by that template");
                    }

                    _installationQueue.AddRange(_postponedToInstall);
                }
            }
            finally
            {
                ClearXmlParserCache();
                _installationQueue.Clear();
                _IDsToBeInstalled.Clear();
                _IDsAlreadyInstalled.Clear();
                this.removeFromDeletionQueue.Clear();
                this.createdItems.Clear();

                var installationArgs = new InstallationEventArgs(installedItems, null, "packageinstall:items:ended");
                Event.RaiseEvent("packageinstall:items:ended", installationArgs);

                installedItems.Clear();
                BlobInstaller.FlushData(ProcessingContext);
            }
        }

        /// <summary>
        /// If an entry contains an item than that item will be added to the installation queue.
        /// </summary>
        /// <param name="entry">The entry.</param>
        public override void Put(PackageEntry entry)
        {
            ItemReference reference = ItemKeyUtils.GetReference(entry.Key);
            if (reference == null)
            {
                Log.Warn("Invalid entry key encountered during installation: " + entry.Key, this);
                return;
            }

            string databaseName = reference.DatabaseName;
            if (_IDsToBeInstalled[databaseName] == null)
            {
                _IDsToBeInstalled[databaseName] = new List<ID>();
            }
            List<ID> list = _IDsToBeInstalled[databaseName];
            if (!list.Contains(reference.ID))
            {
                list.Add(reference.ID);
            }

            _installationQueue.Add(entry);
        }

        bool ItemIsInPackage(Item item)
        {
            string databaseName = item.Database.Name;
            if (_IDsToBeInstalled.ContainsKey(databaseName)
              && _IDsToBeInstalled[databaseName].Contains(item.ID))
            {
                return true;
            }

            if (_IDsAlreadyInstalled.ContainsKey(databaseName)
              && _IDsAlreadyInstalled[databaseName].Contains(item.ID))
            {
                return true;
            }
            return false;
        }

        #endregion

        #region helpers

        #region Protected methods

        /// <summary>
        /// Creates the lightweight item.
        /// </summary>
        /// <param name="item">The item.</param>
        /// <param name="parser">The parser.</param>
        /// <returns>The lightweight item.</returns>
        protected static Item CreateLightweightItem(ItemReference item, XmlVersionParser parser)
        {
            string path = item.Path;
            Database database = item.Database;
            Item result = null;
            if ((path.Length > 0) && (database != null))
            {
                Item parent = database.GetItem(parser.ParentID);

                if (parent == null)
                {
                    parent = GetParentItem(path, database);
                }
                if (parent != null)
                {
                    var created = parser.Created;
                    if (created == DateTime.MinValue)
                    {
                        // Item created date property should be taken from Version created date for items installed from old sitecore package and without Item created field. 
                        created = DateUtil.IsoDateToDateTime(XmlUtil.GetValue(parser.Xml.DocumentElement.SelectSingleNode("fields/field[@key='__created']/content")), created);
                    }

                    result = CreateItem(item, parser, parent, created);

                    if (result == null)
                    {
                        if (parent.Database.GetItem(parser.TemplateID) == null)
                        {
                            throw new Exception(string.Format("Failed to add an item. Key: '{0}'. Reason: there's no template with the following ID '{1}'",
                                                              item.Path,
                                                              parser.TemplateID));
                        }
                        throw new Exception(string.Format("Could not create item. Name: '{0}', ID: '{1}', TemplateID: '{2}', parentId: '{3}'",
                                                            parser.Name, item.ID, parser.TemplateID, parent.ID));
                    }
                }
                else
                {
                    throw new Exception("Could not find target item for: " + path + " (db: " + database.Name + ")");
                }
                result.Versions.RemoveAll(true);
            }
            return result;
        }

        /// <summary>
        /// Gets the item install options.
        /// </summary>
        /// <param name="entry">The entry.</param>
        /// <param name="context">The context.</param>
        /// <param name="prefix">The prefix.</param>
        /// <returns>The item install options.</returns>
        protected virtual BehaviourOptions GetItemInstallOptions(PackageEntry entry, ItemInstallerContext context, string prefix)
        {
            var result = new BehaviourOptions(entry.Properties, prefix);
            if (!result.IsDefined)
            {
                if (context.IsApplyToAll(prefix))
                {
                    result = context.GetInstallOptions(prefix);
                    if (!result.IsDefined)
                    {
                        Log.Error("Installer internal error: item install options not saved after apply-to-all", typeof(ItemInstaller));
                    }
                }
            }
            return result;
        }

        /// <summary>
        /// Gets the prefix.
        /// </summary>
        /// <param name="installedItem">The installed item.</param>
        /// <param name="newItem">The new item.</param>
        /// <returns>The prefix.</returns>
        protected static string GetPrefix(ItemInfo installedItem, ItemInfo newItem)
        {
            if (!installedItem.ID.Equals(newItem.ID))
            {
                return Constants.PathCollisionPrefix;
            }
            return Constants.IDCollisionPrefix;
        }

        /// <summary>
        /// Installs the item.
        /// </summary>
        /// <param name="installOptions">The install options.</param>
        /// <param name="targetItem">The target item.</param>
        /// <param name="item">The item.</param>
        /// <param name="parser">The parser.</param>
        protected void InstallItem(BehaviourOptions installOptions, Item targetItem, ItemReference item, XmlVersionParser parser)
        {
            bool removeVersions;
            this.InstallItem(installOptions, targetItem, item, parser, out removeVersions);
            if (removeVersions)
            {
                RemoveVersions(targetItem, true);
            }
        }

        /// <summary>
        /// Installs the item.
        /// </summary>
        /// <param name="installOptions">The install options.</param>
        /// <param name="targetItem">The target item.</param>
        /// <param name="item">The item.</param>
        /// <param name="parser">The parser.</param>
        /// <param name="removeVersions">Boolean value indicating whether versions of item should be removed.</param>
        protected void InstallItem(BehaviourOptions installOptions, Item targetItem, ItemReference item, XmlVersionParser parser, out bool removeVersions)
        {
            removeVersions = false;
            if (targetItem != null)
            {
                RemoveFromDeletionQueue(targetItem);

                switch (installOptions.ItemMode)
                {
                    case InstallMode.Skip:
                        break;
                    case InstallMode.Overwrite:
                        if (targetItem.ID.Equals(item.ID) || targetItem.TemplateID.Equals(TemplateIDs.Language))
                        {
                            if (item.Path != targetItem.Paths.FullPath)
                            {
                                // REVIEW: Check if parser.ParentID property has sense while calculating parent
                                targetItem.MoveTo(GetParentItem(item.Path, targetItem.Database));
                            }
                            removeVersions = true;
                            EnqueueChildrenForRemove(targetItem);
                            UpdateItemDefinition(targetItem, parser);
                        }
                        else
                        {
                            targetItem.Delete();
                            CreateLightweightItem(item, parser);
                        }
                        break;
                    case InstallMode.SideBySide:
                        CreateLightweightItem(item, parser);
                        break;
                    case InstallMode.Merge:
                        switch (installOptions.ItemMergeMode)
                        {
                            case MergeMode.Append:
                                break;
                            case MergeMode.Clear:
                                removeVersions = true;
                                break;
                            case MergeMode.Merge:
                                break;
                            case MergeMode.Undefined:
                                Error.Assert(false, "Item merge mode is undefined");
                                break;
                        }
                        break;
                    case InstallMode.Undefined:
                        Error.Assert(false, "Item Install mode is undefined");
                        break;
                }
            }
            else
            {
                var createdItem = CreateLightweightItem(item, parser);
                this.RemoveFromDeletionQueue(createdItem);
            }
        }

        /// <summary>
        /// Removes the versions.
        /// </summary>
        /// <param name="targetItem">The target item.</param>
        /// <param name="removeSharedData">if set to <c>true</c> the shared data will be removed.</param>
        protected static void RemoveVersions(Item targetItem, bool removeSharedData)
        {
            targetItem.Database.Engines.DataEngine.RemoveData(targetItem.ID, Language.Invariant, removeSharedData);
        }

        #endregion

        #region Caching of XmlVersionParser

        #region Fields

        readonly SafeDictionary<PackageEntry, XmlVersionParser> _parserCache = new SafeDictionary<PackageEntry, XmlVersionParser>();

        #endregion

        #region Private methods

        void AddToParserCache(PackageEntry entry, XmlVersionParser parser)
        {
            if (!_parserCache.ContainsKey(entry))
            {
                _parserCache.Add(entry, parser);
            }
        }

        void ClearXmlParserCache()
        {
            _parserCache.Clear();
        }

        XmlVersionParser GetXmlVersionParser(PackageEntry entry)
        {
            if (_parserCache.ContainsKey(entry))
            {
                return _parserCache[entry];
            }
            return new XmlVersionParser(entry);
        }

        #endregion

        #endregion

        #region Private methods

        private void AddToPostponedList(PackageEntry entry)
        {
            this._postponedToInstall.Add(entry);
        }

        BehaviourOptions AskUserForInstallOptions(ItemInfo installedItem, ItemInfo newItem, ItemInstallerContext context)
        {
            try
            {
                Pair<BehaviourOptions, bool> result = context.Events.AskUser(installedItem, newItem, ProcessingContext);
                // save modes
                string prefix = GetPrefix(installedItem, newItem);
                context.ApplyToAll[prefix] = result.Part2;
                context.GetInstallOptions(prefix).Assign(result.Part1);
                return result.Part1;
            }
            catch (ThreadAbortException)
            {
                throw;
            }
            catch (Exception e)
            {
                throw new Exception("Could not query outer context (or user) for overwrite options. Most probably ItemInstallerContext.Events is not set", e);
            }
        }

        static KeyValuePair<string, string> BuildCollectionKey(Item source)
        {
            return new KeyValuePair<string, string>(source.Database.Name, source.ID.ToString());
        }

        private void EnqueueChildrenForRemove(Item parentItem)
        {
            foreach (Item child in parentItem.GetChildren(ChildListOptions.IgnoreSecurity))
            {
                var collectionKey = BuildCollectionKey(child);
                if (this._pendingDeleteItems.Any(i => i.Key == collectionKey.Key && i.Value == collectionKey.Value) ||
                  this.removeFromDeletionQueue.Any(i => i.Key == collectionKey.Key && i.Value == collectionKey.Value))
                {
                    continue;
                }

                this._pendingDeleteItems.Add(collectionKey);
            }
        }

        static string GetKey(ItemReference itemRef)
        {
            return itemRef.DatabaseName + ":" + itemRef.Path;
        }

        static Item GetParentItem(string path, Database database)
        {
            if (database != null)
            {
                string parentPath = StringUtil.GetLongestPrefix(path, Constants.KeySeparator);
                if (parentPath.Length > 0)
                {
                    return database.CreateItemPath(parentPath);
                }
            }
            return null;
        }

        /// <summary>
        /// Gets the version install mode.
        /// </summary>
        /// <param name="entry">The entry.</param>
        /// <param name="reference">The reference.</param>
        /// <param name="parser">The parser.</param>
        /// <param name="context">The context.</param>
        /// <param name="removeVersions">Boolean value indicating whether versions of item should be removed.</param>
        /// <returns>
        /// The version install mode.
        /// </returns>
        private VersionInstallMode GetVersionInstallMode(PackageEntry entry, ItemReference reference, XmlVersionParser parser, ItemInstallerContext context, out bool removeVersions)
        {
            removeVersions = false;

            if (this.createdItems.Contains(reference.ID))
            {
                bool ignorePathCollision;
                Item item = this.GetTargetItem(reference, out ignorePathCollision);
                if (item != null)
                {
                    this.RemoveFromDeletionQueue(item);
                    context.VersionInstallMode = Sitecore.Install.Items.VersionInstallMode.Append;
                    return context.VersionInstallMode;
                }
            }

            ItemInfo newItem = new ItemInfo(reference, parser);
            if (!newItem.ID.Equals(context.CurrentItemID))
            {
                //installing new item
                bool ignorePathCollision;
                Item item = GetTargetItem(reference, out ignorePathCollision);
                if (item != null)
                {
                    ItemInfo installedItem = new ItemInfo(item);
                    // if item exists
                    string prefix = GetPrefix(installedItem, newItem);
                    //get install item modes. If modes is not defined ask user
                    BehaviourOptions installOptions = GetItemInstallOptions(entry, context, prefix);
                    if (!installOptions.IsDefined)
                    {
                        installOptions = AskUserForInstallOptions(installedItem, newItem, context);
                    }
                    context.VersionInstallMode = installOptions.GetVersionInstallMode();
                    InstallItem(installOptions, item, reference, parser, out removeVersions);
                }
                else
                {
                    context.VersionInstallMode = VersionInstallMode.Append;
                    InstallMode installMode = ignorePathCollision ? InstallMode.SideBySide : InstallMode.Overwrite;
                    InstallItem(new BehaviourOptions(installMode, MergeMode.Undefined), null, reference, parser, out removeVersions);
                }
                context.CurrentItemID = item != null ? item.ID.ToString() : newItem.ID;

                string dbName = reference.DatabaseName;
                if (!_IDsAlreadyInstalled.ContainsKey(dbName))
                {
                    _IDsAlreadyInstalled.Add(dbName, new List<ID>());
                }
                _IDsAlreadyInstalled[dbName].Add(ID.Parse(newItem.ID));
                if (item != null && !_IDsAlreadyInstalled[dbName].Contains(item.ID))
                {
                    _IDsAlreadyInstalled[dbName].Add(item.ID);
                }
            }

            return context.VersionInstallMode;
        }

        /// <summary>
        /// Installs the entry.
        /// </summary>
        /// <param name="entry">The entry.</param>
        /// <exception cref="System.Exception">ItemInstallerContext is not set in current processing context.</exception>
        private void InstallEntry(PackageEntry entry)
        {
            ItemReference reference = ItemKeyUtils.GetReference(entry.Key);

            reference = reference.Reduce();

            XmlVersionParser parser = this.GetXmlVersionParser(entry);

            if (parser.TemplateID != reference.ID && (this.ItemIsFurtherInList(parser.TemplateID, reference.DatabaseName) || this.BaseTemplateIsFurtherInList(parser.BaseTemplates, reference.DatabaseName)))
            {
                bool ignorePathCollision;
                Item item = this.GetTargetItem(reference, out ignorePathCollision);
                if (item == null)
                {
                    CreateLightweightItem(reference, parser);
                    this.createdItems.Add(reference.ID);
                }

                this.AddToParserCache(entry, parser);
                this.AddToPostponedList(entry);
                return;
            }

            if (this.Context == null)
            {
                throw new Exception("ItemInstallerContext is not set in current processing context.");
            }

            try
            {
                bool removeVersions;
                VersionInstallMode mode = this.GetVersionInstallMode(entry, reference, parser, Context, out removeVersions);
                if (mode != VersionInstallMode.Undefined)
                {
                    if (mode != VersionInstallMode.Skip)
                    {
                        Log.Info("Installing item: " + entry.Key, this);
                        var itemReference = ParseRef(entry.Key);
                        installedItems.Add(new ItemUri(ID.Parse(Context.CurrentItemID), itemReference.Language, itemReference.Version, itemReference.Database));
                        //VersionInstaller.PasteVersion(parser.Xml.DocumentElement, reference.GetItem(), mode, this.ProcessingContext, removeVersions);
                        VersionInstaller.PasteVersion(parser.Xml.DocumentElement, itemReference.GetItem(), mode, this.ProcessingContext, removeVersions);
                    }
                }
                else
                {
                    Log.Info(string.Format("Version install mode is not defined for entry '{0}'", entry.Key), this);
                }
            }
            catch (ThreadAbortException)
            {
                Log.Info("Installation was aborted at entry: " + entry.Key, this);
                throw;
            }
            catch (Exception ex)
            {
                Log.Error("Error installing " + entry.Key, ex, this);
                throw;
            }

            this._IDsToBeInstalled[reference.DatabaseName].Remove(reference.ID);
        }

        private bool BaseTemplateIsFurtherInList(ID[] baseTemplates, string databaseName)
        {
            if (this._IDsToBeInstalled.ContainsKey(databaseName))
            {
                return baseTemplates.Any(id => this._IDsToBeInstalled[databaseName].Contains(id));
            }
            return false;
        }

        bool ItemIsFurtherInList(ID itemID, string databaseName)
        {
            return _IDsToBeInstalled.ContainsKey(databaseName)
                   && _IDsToBeInstalled[databaseName].Contains(itemID);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="reference"></param>
        /// <returns></returns>
        public static ItemReference ParseRef(string reference)
        {
            string[] parts = reference.Split('/');
            string database = parts[1];
            string version = parts[parts.Length - 2];
            string languageString = parts[parts.Length - 3];
            string id = parts[parts.Length - 4];

            string[] pathParts = new string[parts.Length - 5];
            pathParts[0] = parts[0];
            Array.Copy(parts, 2, pathParts, 1, parts.Length - 6);
            string path = string.Join("/", pathParts);

            Language language = string.Compare(languageString, "invariant", StringComparison.InvariantCultureIgnoreCase) == 0 ? Language.Invariant : Language.Parse(languageString);

            return new ItemReference(database, path, id.Length == 0 ? null : ID.Parse(id), language, Data.Version.Parse(version));
        }

        /// <summary>
        /// This method returns an item by a reference, but ignores items from the current package which may cause a path collision.
        /// </summary>
        /// <param name="reference">The reference.</param>
        /// <param name="ignorePathCollision">if set to <c>true</c> a path collision should be ignored.</param>
        /// <returns></returns>
        [CanBeNull]
        Item GetTargetItem(ItemReference reference, out bool ignorePathCollision)
        {
            ignorePathCollision = false;

            Item item = reference.GetItem();
            if (item == null
              || item.ID == reference.ID
              || item.Parent == null
              || !ItemIsInPackage(item))
            {
                return item;
            }
            // Searching for an item, whith the same name but not belongs to the package
            Item parentItem = item.Parent;
            foreach (Item child in parentItem.Children)
            {
                if (child.Name != item.Name
                  || child.ID == item.ID
                  || ItemIsInPackage(child))
                {
                    continue;
                }
                ignorePathCollision = true;
                return child;
            }
            return null;
        }

        private void RemoveFromDeletionQueue(Item item)
        {
            KeyValuePair<string, string> key = BuildCollectionKey(item);
            this._pendingDeleteItems.Remove(key);
            this.removeFromDeletionQueue.Add(key);
        }

        static void UpdateItemDefinition(Item targetItem, XmlVersionParser parser)
        {
            Item version = VersionInstaller.ParseItemVersion(parser.Xml.DocumentElement, targetItem, VersionInstallMode.Undefined);
            targetItem.Editing.BeginEdit();
            targetItem.Name = version.Name;
            targetItem.BranchId = version.BranchId;
            targetItem.TemplateID = version.TemplateID;
            targetItem.RuntimeSettings.ReadOnlyStatistics = true;
            targetItem.Editing.EndEdit();
        }

        internal static Func<ItemReference, XmlVersionParser, Item, DateTime, Item> CreateItem
        {
            get { return createItem; }
            set { createItem = value; }
        }

        #endregion

        #endregion helpers

        #region properties

        /// <summary>
        /// Gets the pended to be deleted items.
        /// </summary>
        /// <value>The pending delete items.</value>
        public IList<KeyValuePair<string, string>> PendingDeleteItems
        {
            get
            {
                return _pendingDeleteItems;
            }
        }

        #endregion properties

        #region Protected methods

        /// <summary>
        /// Creates the context.
        /// </summary>
        /// <returns>The context.</returns>
        protected override ItemInstallerContext CreateContext()
        {
            IItemInstallerEvents events;
            if (!ProcessingContext.HasAspect<IItemInstallerEvents>())
            {
                events = new DefaultItemInstallerEvents(new BehaviourOptions(InstallMode.Merge, MergeMode.Clear));
            }
            else
            {
                events = ProcessingContext.GetAspect<IItemInstallerEvents>();
            }
            return new ItemInstallerContext(events);
        }

        #endregion

        #region Nested type: ContentRestorer

        /// <summary>
        /// Content restorer. Deletes item which is pended to be deleted
        /// </summary>
        class ContentRestorer : IProcessor<IProcessingContext>, IDisposable
        {
            #region Fields

            readonly IList<KeyValuePair<string, string>> pendingDeleteItems;

            #endregion

            #region Constructors

            /// <summary>
            /// Initializes a new instance of the <see cref="ContentRestorer"/> class.
            /// </summary>
            /// <param name="pendingDeleteItems">The pending delete items.</param>
            public ContentRestorer(IList<KeyValuePair<string, string>> pendingDeleteItems)
            {
                this.pendingDeleteItems = pendingDeleteItems;
            }

            #endregion

            #region Public methods

            /// <summary>
            /// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
            /// </summary>
            public void Dispose()
            {
                pendingDeleteItems.Clear();
            }

            /// <summary>
            /// Runs the processor.
            /// </summary>
            /// <param name="entry">The entry.</param>
            /// <param name="context">The processing context.</param>
            public void Process(IProcessingContext entry, IProcessingContext context)
            {
                foreach (KeyValuePair<string, string> pair in pendingDeleteItems)
                {
                    try
                    {
                        Database database = Factory.GetDatabase(pair.Key);
                        if (database != null)
                        {
                            Item pendingDeleteItem = database.Items[pair.Value];
                            if (pendingDeleteItem != null)
                            {
                                pendingDeleteItem.Delete();
                            }
                            else
                            {
                                Log.Error("Error finding item: [" + pair.Key + "]: " + pair.Value, this);
                            }
                        }
                        else
                        {
                            Log.Error("Error finding database: [" + pair.Key + "]", this);
                        }
                    }
                    catch (Exception)
                    {
                        Log.Error("Error deleting item: [" + pair.Key + "]: " + pair.Value, this);
                    }
                }
            }

            #endregion
        }

        #endregion

        #region Nested type: VersionInstaller

        /// <summary>
        /// Version installer
        /// </summary>
        public static class VersionInstaller
        {
            #region Public methods

            /// <summary>
            /// Pastes the version.
            /// </summary>
            /// <param name="versionXml">The version XML.</param>
            /// <param name="target">The target.</param>
            /// <param name="mode">The mode.</param>
            /// <param name="context">The context.</param>
            public static void PasteVersion(XmlNode versionXml, Item target, VersionInstallMode mode, IProcessingContext context)
            {
                PasteVersion(versionXml, target, mode, context, false);
            }

            /// <summary>
            /// Pastes the version.
            /// </summary>
            /// <param name="versionXml">The version XML.</param>
            /// <param name="target">The target.</param>
            /// <param name="mode">The mode.</param>
            /// <param name="context">The context.</param>
            /// <param name="removeOtherVersions">if set to <c>true</c> other versions will be removed.</param>
            public static void PasteVersion(XmlNode versionXml, Item target, VersionInstallMode mode, IProcessingContext context, bool removeOtherVersions)
            {
                Error.AssertObject(versionXml, "xml");
                Error.AssertObject(target, "target");
                Error.Assert(mode == VersionInstallMode.Append || mode == VersionInstallMode.Merge, "Unknown version install mode");

                Item version = ParseItemVersion(versionXml, target, mode, removeOtherVersions);
                Error.AssertObject(version, "versions");
                BlobInstaller.UpdateBlobData(version, context);

                // Fix for Bug 90254
                UpdateFieldSharing(version, target);

                InstallVersion(version);

                if (removeOtherVersions)
                {
                    Item item = target.Database.GetItem(version.ID);
                    foreach (var itemVersion in item.Versions.GetVersions(true))
                    {
                        if (itemVersion.Version != version.Version || itemVersion.Language != version.Language)
                        {
                            itemVersion.Versions.RemoveVersion();
                        }
                    }
                }
            }

            #endregion

            #region private scope

            #region Public methods

            /// <summary>
            /// Parses the item version.
            /// </summary>
            /// <param name="versionNode">The version node.</param>
            /// <param name="target">The target item.</param>
            /// <param name="mode">The version install mode.</param>
            /// <returns>The item version.</returns>
            public static Item ParseItemVersion(XmlNode versionNode, Item target, VersionInstallMode mode)
            {
                return ParseItemVersion(versionNode, target, mode, false);
            }

            /// <summary>
            /// Parses the item version.
            /// </summary>
            /// <param name="versionNode">The version node.</param>
            /// <param name="target">The target item.</param>
            /// <param name="mode">The version install mode.</param>
            /// <param name="removeOtherVersions">if set to <c>true</c> other versions will be removed.</param>
            /// <returns>
            /// The item version.
            /// </returns>
            public static Item ParseItemVersion(XmlNode versionNode, Item target, VersionInstallMode mode, bool removeOtherVersions)
            {
                string name = XmlUtil.GetAttribute("name", versionNode);
                string language_str = XmlUtil.GetAttribute("language", versionNode);
                string version_str = XmlUtil.GetAttribute("version", versionNode);

                ID templateID = MainUtil.GetID(XmlUtil.GetAttribute("tid", versionNode));
                ID branchId = MainUtil.GetID(XmlUtil.GetAttribute("mid", versionNode));

                if (ID.IsNullOrEmpty(branchId))
                {
                    branchId = MainUtil.GetID(XmlUtil.GetAttribute("bid", versionNode));
                }

                DateTime created = DateUtil.IsoDateToDateTime(XmlUtil.GetAttribute("created", versionNode), DateTime.MinValue);

                CoreItem.Builder builder = new CoreItem.Builder(target.ID, name, templateID, target.Database.DataManager);

                Language language;

                if (!Language.TryParse(language_str, out language))
                {
                    language = Language.Invariant;
                }

                builder.SetLanguage(language);
                switch (mode)
                {
                    case VersionInstallMode.Append:
                        if (removeOtherVersions)
                        {
                            version_str = Data.Version.First.ToString();
                        }
                        else
                        {
                            //Item langItem = target.Database.Items[target.ID, language];
                            if (target != null)
                            {
                                Data.Version version = target.Version;
                                if (version != null)
                                {
                                    version_str = version.Number.ToString();
                                }
                                else
                                {
                                    version_str = Data.Version.First.ToString();
                                }
                            }
                            else
                            {
                                version_str = Data.Version.First.ToString();
                            }
                        }

                        break;
                }
                builder.SetVersion(Data.Version.Parse(version_str));
                builder.SetBranchId(branchId);
                builder.SetCreated(created);
                XmlNodeList fields = versionNode.SelectNodes("fields/field");
                foreach (XmlNode field in fields)
                {
                    ParseField(field, builder);
                }

                return new Item(builder.ItemData.Definition.ID, builder.ItemData, target.Database);
            }

            #endregion

            #region Private methods

            /// <summary>
            /// Installs the version.
            /// </summary>
            /// <param name="version">The version.</param>
            static void InstallVersion(Item version)
            {
                if (version != null)
                {
                    version.Editing.BeginEdit();
                    version.RuntimeSettings.ReadOnlyStatistics = true;
                    version.RuntimeSettings.SaveAll = true;
                    version.Editing.EndEdit(false, true);
                    Item item = version.Database.GetItem(version.Uri.ToDataUri());
                    item.Editing.BeginEdit();
                    item.Name = version.InnerData.Definition.Name;
                    item.TemplateID = version.InnerData.Definition.TemplateID;
                    item.BranchId = version.InnerData.Definition.BranchId;
                    item.RuntimeSettings.ReadOnlyStatistics = true;
                    item.Editing.EndEdit();

                    string isoNow = DateUtil.IsoNowWithTicks;

                    bool badCreated = string.Compare(item[FieldIDs.Created], isoNow, StringComparison.OrdinalIgnoreCase) > 0; // date in the future
                    bool badUpdated = string.Compare(item[FieldIDs.Updated], isoNow, StringComparison.OrdinalIgnoreCase) > 0;

                    if (badCreated || badUpdated)
                    {
                        item.Editing.BeginEdit();
                        item.RuntimeSettings.ReadOnlyStatistics = true;

                        if (badCreated)
                        {
                            item[FieldIDs.Created] = isoNow;
                        }

                        if (badUpdated)
                        {
                            item[FieldIDs.Updated] = isoNow;
                        }

                        item.Editing.EndEdit();
                    }
                }
            }

            /// <summary>
            /// Parses the field.
            /// </summary>
            /// <param name="node">The node.</param>
            /// <param name="builder">The builder.</param>
            static void ParseField(XmlNode node, CoreItem.Builder builder)
            {
                ID tID = ID.Parse(XmlUtil.GetAttribute("tfid", node));
                XmlNode content = node.SelectSingleNode("content");
                if (content != null)
                {
                    string contentValue = XmlUtil.GetValue(content);
                    builder.AddField(tID, contentValue);
                }
            }


            /// <summary>
            /// Initiates the TemplateEngine.ChangeFieldSharing process.
            /// </summary>
            /// <param name="source">The source.</param>
            /// <param name="target">The target.</param>
            private static void UpdateFieldSharing(Item source, Item target)
            {
                if (source.TemplateID != TemplateIDs.TemplateField)
                {
                    return;
                }

                ItemReference reference = new ItemReference(target);
                reference.Language = source.Language;
                target = reference.GetItem();

                target.Editing.BeginEdit();
                target.Fields[TemplateFieldIDs.Shared].Value = source.Fields[TemplateFieldIDs.Shared].Value;
                target.Fields[TemplateFieldIDs.Unversioned].Value = source.Fields[TemplateFieldIDs.Unversioned].Value;
                target.Editing.EndEdit();
            }

            #endregion

            #endregion private scope
        }

        #endregion
    }
}